# RDMA replication ACK over RDMA

## 用語定義

- TCP replication:
  レプリケーションデータ本体を TCP で送受信する方式。
- RDMA replication:
  レプリケーションデータ本体を RDMA で送受信する方式。
- TCP ACK:
  ACK を TCP 経路で返すこと。
  この TODO では主に、「RDMA replication に対する ACK を TCP で返している現状」を指して使う。
- RDMA ACK:
  ACK を RDMA 経路で返すこと。
  この TODO では主に、「RDMA replication に対する ACK を RDMA で返すように変更する」という意味で使う。
- ACK:
  ここでいう ACK は、レプリケーションデータの受信・処理完了に対する既存の確認応答を指す。
  ACK 自体の意味は既存実装と同じであり、新しい ACK 種別や protocol 上の新しい意味を導入することは前提としない。
- ACK 用 buffer:
  rdma-comm-lib は ACK 専用 buffer を別建てで確保せず、通常の送受信 buffer の後半を
  ACK 領域として流用する仕様。buffer の 2x 確保 (前半 data 領域・後半 ACK 領域) は
  ライブラリ内部で行うため、limestone 側はサイズ計算を意識する必要はなく、config の
  `region_size_bytes` には data 領域サイズを渡せばよい。実メモリ使用量が指定値の
  2 倍になる点のみ留意する。
  remote DMA address の事前交換は rdma-comm-lib 利用側 (limestone) の責務。

## 背景

- レプリケーションの経路には、TCP replication と RDMA replication がある。
- 現状、RDMA replication ではレプリケーションデータ本体は RDMA で送受信しているが、ACK は TCP で返している。
- この作業では、RDMA replication に対する ACK を TCP ではなく RDMA で返すように変更する。

## 現状整理

### rdma-comm-lib 側の前提（与件）

rdma-comm-lib の `feat/rdma-ack-over-rdma` ブランチで RDMA ACK 化 (Phase 1) と
per-session TCP socket 全廃 (Phase 2) のうち主要作業がほぼ完了している。
master へは未マージだが、新 I/F のヘッダ・ライブラリは `find_package(rdma_comm)` が
解決する install 先 (`/home/umegane/opt`) に展開済みで、limestone からそのまま参照
できる状態にある。

新 I/F 仕様の要点:

- TCP ACK 経路はライブラリから完全撤去。`rdma_receiver::initialize()` の
  `receive_handler` / `receive_handler_with_ack` のいずれを選んでも RDMA ACK 経路を使う。
- `rdma_receiver::register_channel` API は削除済み。channel_id は決定論的に
  `0..N-1` (N = `channel_count`) を sender/receiver 双方で共有する。
- `rdma_sender::get_send_stream(channel_id)` は fd 引数を取らない。
- `read_ack_message_from_fd` / `write_ack_message_to_fd` / `safe_writer` は削除済み。
- 初期化は SETUP フェーズ → `rdma_receiver::finalize_channel_setup_with_sender(rdma_sender*)`
  → `rdma_sender::finalize_channel_setup()` → TRANSFER フェーズ の順で確定。
  finalize 後の channel 追加登録は不可。
- buffer の 2x 確保 (前半 data 領域・後半 ACK 領域) は rdma-comm-lib が内部で行う
  (vendor 既知バグ workaround)。limestone 側は config に `region_size_bytes` (data
  領域サイズ) を渡すだけでよく、サイズ計算上の責務はない。ただし、ACK 用 buffer の
  remote DMA address の事前交換は rdma-comm-lib 利用側 (limestone) の責務。
- `flush()` の semantics は不変 (ACK 受信完了まで待機)。受信側 callback の戻り値で
  ACK body を返す手段も既存どおり利用可能。

### limestone 側の現状

新 I/F に追随しておらず、`find_package` の参照先を新 I/F へ向けた瞬間にビルドが通らない。
ぶつかる主な箇所:

- `src/limestone/replication/replica_server.cpp` の `rdma_receiver_->register_channel(channel_id, client_fd)` 呼び出し
  (通常経路と pending 経路の 2 箇所)。`pending_rdma_channels_` 機構ごと不要。
- `src/limestone/rdma/rdma_sender_base.h::get_send_stream(channel_id, ack_fd)` の I/F、ならびに
  `rdma_comm_sender` / `null_rdma_sender` 実装の `ack_fd` 引数 (および `dup(ack_fd)`)。
- `src/limestone/rdma/rdma_receiver_base.h::register_channel(channel_id, ack_socket)` の I/F、ならびに
  `rdma_comm_receiver` / `null_rdma_receiver` 実装。
- `src/limestone/replication/channel_handler_base.cpp::send_ack()` のうち log channel
  (`log_channel_handler.cpp:64`) から呼ばれる経路。control channel handshake 用は別扱い。
- `src/limestone/datastore_impl.cpp::set_rdma_ack_fd_for_test` / `rdma_ack_fd_for_test_`
  (用途消失)。

### 今回の作業の論点

1. ライブラリ仕様 (与件) に追随して build を回復させる。
2. ACK 用 buffer の確保と remote DMA address の双方向交換を limestone 側に新設する。
3. SETUP/TRANSFER フェーズ移行 (`finalize_channel_setup_with_sender` /
   `finalize_channel_setup`) を初期化フローへ組み込む。
4. log channel の ACK 返送を `send_ack()` 経由から
   `receive_handler_with_ack` の戻り値経由に切り替える。
5. RDMA replication における `log_channel` の責務を整理し、不要部分を外す。

## 作業方針

### 1. API 追随修正 (build 回復)

- 開発環境では新 I/F が `/home/umegane/opt` に install 済み。CI 側および他開発者の
  環境で参照解決先が揃うかは別途確認する (rdma-comm-lib master 未マージのため、
  CI が安定 release を引いている場合は先に CI 側の依存を切り替える)。
- `rdma_sender_base` から `ack_fd` 引数を削除し、`rdma_receiver_base::register_channel`
  を I/F ごと撤去する (channel_id ベースの登録は不要になったため)。
- `rdma_comm_sender` / `rdma_comm_receiver` / `null_rdma_sender` / `null_rdma_receiver` の
  シグネチャを揃える。`rdma_comm_sender::get_send_stream` の `dup(ack_fd)` も削除。
- `replica_server.cpp` の `rdma_receiver_->register_channel(...)` 呼び出し 2 箇所を削除。
  あわせて `pending_rdma_channels_` 機構 (deferred 登録) を撤去する。
- `datastore_impl::set_rdma_ack_fd_for_test` / `rdma_ack_fd_for_test_` を削除。
  関連するテストフックも整理する。
- `rdma_receiver::initialize` の overload は、ACK body を使うかどうかで選択する。
  RDMA replication の log channel ACK は受信処理結果 (成否・原因) を sender に
  返す必要があるため `receive_handler_with_ack` を選ぶ前提で進める。

この段階の完了条件: 新 I/F 参照下でビルドとリンクが通り、既存の TCP replication の
単体テストが全て pass する状態。RDMA replication 側はまだ動かなくてよい。

### 2. ACK 用 buffer の確保と DMA address 交換

#### lib 仕様の再確認 (前提の訂正)

ライブラリ実装 (`rdma-comm-lib` `feat/rdma-ack-over-rdma`) を読んで分かった事実:

- **各 side で `rdma_sender` と `rdma_receiver` の両 instance が必要**。当初想定していた
  「sender に ACK 受信 buffer accessor を生やす」「`get_dma_address` の意味を再定義する」
  という方向は誤りだった。lib の構造上、ACK 受信は別 `rdma_receiver` instance、
  ACK 送信は別 `rdma_sender` instance で担う:
  - leader (data 送信側): `data_sender (rdma_sender)` + `ack_receiver (rdma_receiver)`
  - replica (data 受信側): `data_receiver (rdma_receiver)` + `ack_sender (rdma_sender)`
  - 同 side で `receiver.finalize_channel_setup_with_sender(sender)` を呼んで結線する。
    receiver はこの sender 経由で ACK frame を送出 (`submit_ack_write`) し、
    自分宛に届いた ACK frame を sender の send_stream へ配送
    (`deliver_ack_to_send_stream`)。
- **buffer の 2x 確保は base lib の挙動**で、`blob_relay` 拡張の話ではない。
  `src/rdma_sender_detail.cpp` / `src/rdma_receiver_detail.cpp` の内部で
  `2 * region_size_bytes` を確保する。
- 各 instance が 2x 確保するため、片方向 RDMA Write しか使わない limestone では
  side あたり実メモリ 4×region_size を消費し、有効利用は 2×region_size に留まる
  (sender.local 上半分・receiver.shared 上半分が片方向用途では片方しか使われない)。

  | side | sender.local (2x) | receiver.shared (2x) |
  |---|---|---|
  | leader | 下=data 送信源 ✓ / 上=未使用 | 下=未使用 / 上=ACK 受信先 ✓ |
  | replica | 下=未使用 / 上=ACK 送信源 ✓ | 下=data 受信先 ✓ / 上=未使用 |

  この浪費は lib 仕様起因なので「将来の lib 改善要望」として下記「積み残し」に転記する。
  今回 §2 ではこのまま受け入れて進める。

#### handshake 拡張内容

- 現在の handshake は `RDMA_INIT (leader → replica): { slot_count }` →
  `RDMA_INIT_ACK (replica → leader): { replica_data_dma_address }` の片方向交換。
- §2 では `RDMA_INIT` を双方向化する (採択: 案 A):
  ```
  RDMA_INIT     (leader → replica): { slot_count, leader_ack_dma_address }
  RDMA_INIT_ACK (replica → leader): { replica_data_dma_address }   // 既存維持
  ```
- 1 RTT で双方向のアドレス交換を完結させる。`replication protocol version` は
  RDMA replication が実機投入前であることから据え置き (破壊的変更を許容)。
- `slot_count` は §2 では現状維持 (master 側のみ参照する定数だが、replica が値を
  受け取る経路は残す)。N=512 固定化に伴う構造削除は §3 で行う。

#### handshake 順序 (timing 制約)

ACK 用 instance の `initialize()` には対向の DMA address が必要。順序:

1. leader: `ack_receiver_->initialize(no_op_handler)` → leader_ack_dma_address 取得
2. leader → replica: `RDMA_INIT { slot_count, leader_ack_dma_address }`
3. replica: `data_receiver_->initialize(on_rdma_receive)` → replica_data_dma_address 取得
4. replica: `ack_sender_->initialize(leader_ack_dma_address)`
5. replica → leader: `RDMA_INIT_ACK { replica_data_dma_address }`
6. leader: `data_sender_->initialize(replica_data_dma_address)`

§2 では (1)〜(6) の `initialize` までで完了とする。
`finalize_channel_setup_with_sender` / `finalize_channel_setup` は §3。

#### この順序で FIN なしに race が起きない理由

一般に handshake の最後に FIN (両 side ready 確認) が無いと、A 側 init 完了直後の
RDMA Write が B 側 callback 登録前に到着して drop される race が起き得る。
本順序ではそれが構造的に起こらないことを 2 方向で確認しておく:

- **leader → replica の data RDMA Write**: 物理的に発行可能になるのは step 6
  (leader の `data_sender_->initialize`) 以降。replica の `data_receiver_->initialize`
  (callback 登録) は step 3 で完了済みで、step 5 の TCP `RDMA_INIT_ACK` 返信は
  step 4 の後 → leader が step 6 を実行できるのは step 5 受信後 → step 3 完了が
  保証される。data callback drop は発生しない。
- **replica → leader の ACK RDMA Write**: 物理的に発行可能になるのは step 4 以降。
  leader の `ack_receiver_->initialize` は step 1 で完了済みなので、ACK 受信 buffer
  は step 4 より前に登録済。callback drop は発生しない。
  なお §2 では `finalize_channel_setup_with_sender` を呼ばないので lib 内部の
  `submit_ack_write` 経路自体が遮断されており、ACK は実際には発行されない。

要点: limestone は片方向 RDMA Write しか使わないので、「先に rx 側 init →
次に opposite 側 tx init」の順を保てば race が構造的に消える。双方向 RDMA Write を
使うアプリ (例: blob_relay) では同じ前提が成り立たないため別途 FIN が必要になる。

#### 実装上の決定

- 新設する ack_receiver / ack_sender は既存抽象 `rdma_receiver_base` /
  `rdma_sender_base` を流用する。null impl と rdma_comm impl の両方にシグネチャを
  揃え、CI (ENABLE_RDMA=OFF) でも null 経路でビルド可能にする。
- leader 側 `ack_receiver` の receive callback は no-op で良い (届く RDMA frame は
  すべて lib 内部で `deliver_ack_to_send_stream` に流れ、user callback は呼ばれない)。
- §2 の段階では `finalize_channel_setup_with_sender` 等は呼ばないため、ack_receiver
  に届いた ACK frame は捨てられる (lib 内部の `rdma_sender_` が未設定で
  `send_acknowledgement` が拒否する)。実 transfer は §3 で動作するようになる。

#### 完了条件

server / client それぞれが、自身の ACK 用 instance を持ち、対向の ACK buffer
DMA address を入手して `initialize()` まで到達している。
データ転送・ACK 転送はまだ動かなくてよい。

### 3. SETUP / TRANSFER フェーズ移行の組み込み

- channel 数 N は定数 512 で固定。初期化時に 0..511 の channel をまとめて
  pre-create し、SETUP フェーズ中に bulk 登録する。LOG_CHANNEL_CREATE で動的に
  channel を増やす構造は撤去する。
- `replica_server` 側 (受信): RDMA receiver 初期化 → 512 channel の bulk 登録 →
  `finalize_channel_setup_with_sender(sender)` の順で進める。
- `datastore_impl` 側 (送信): 同様に 512 channel 分の `get_send_stream` を発行した
  後、`rdma_sender::finalize_channel_setup()` を呼ぶ。
- 既存の deferred 登録 (`pending_rdma_channels_`) は撤去する (bulk 登録への置換)。
- 512 を超える `LOG_CHANNEL_CREATE` 要求が来た場合は、limestone 側でエラーを返す。
  finalize 後に新規 channel が登録されようとした場合の拒否はライブラリ側にも仕掛けが
  あるが、limestone 側でも上位に伝えられるよう整える。

#### finalize ordering と FIN 相当の確認

§3 では §2 と異なり、finalize 完了の双方向確認 (FIN 相当の TCP message) が必要に
なりそう。理由:

- replica が `data_receiver_->finalize_channel_setup_with_sender(ack_sender_)` を
  呼ぶ前に leader の data_sender が data RDMA Write を発行すると、replica は受信
  自体はできるが ACK 経路 (`submit_ack_write`) が遮断されており ACK を返せない。
  結果として leader の `flush()` が timeout する。
- 一方 leader の `data_sender_->finalize_channel_setup()` は SETUP→TRANSFER 遷移で
  あって、これより前から `get_send_stream()` も `send_bytes()` も成立してしまう
  (lib の I/F コメント参照)。
- ∴「**replica の finalize 完了 → leader の finalize 完了 → 実 transfer 開始**」の
  順序を保証する仕掛けが必要。素直な実装は、replica が finalize 完了後に control
  channel 経由で `RDMA_FINALIZE_ACK` (仮称) を leader に返し、leader はそれを
  受信してから自身の finalize を実行する形。

具体の wire format / message 種別は §3 着手時に確定する。本 TODO に論点として残す。

この段階の完了条件: 初期化シーケンスが SETUP → finalize → TRANSFER の順で確定し、
flush() が RDMA ACK で完了するようになる。

### 4. log channel での TCP `send_ack()` 呼び出し削除

- 新 lib は receive callback を呼んだ時点で RDMA ACK を自動返送する。よって
  RDMA replication の log channel から `channel_handler_base::send_ack()` を経由
  して TCP socket に ACK を書く経路は redundant となり、leader 側 lib も読まないため
  単なる死コード化する。
- `log_channel_handler.cpp:64` の `send_ack()` 呼び出し (RDMA 経路に到達するもの) を
  削除する。TCP replication 側の log channel は引き続き TCP ACK を返す必要があるため、
  TCP/RDMA 経路の分岐を整理した上で、RDMA 経路でだけ呼ばないようにする
  (もしくは log_channel_handler 自体を経路ごとに分離する)。
- `channel_handler_base::send_ack()` 自体は control channel の初期 handshake ACK で
  使われ続けるため、関数自体は残す。RDMA log channel から呼ばれなくなることで責務が
  自然に縮小する形になる。
- `receive_handler` と `receive_handler_with_ack` の選択は ACK 送信自体とは独立した
  別論点。lib は body の有無に関わらず ACK を返す。limestone が処理結果 (成否・原因)
  を leader に伝えたい場合のみ `receive_handler_with_ack` を選び、callback の戻り値
  `optional<ack_body>` を使う。今回はまず最低限の `receive_handler` で進め、
  必要に応じて後段で body 経由に切り替える方針とする。
- 不正 payload や sequence gap など lib 内部で検出されるエラーは lib 側で failure ACK
  を返す。limestone 側 callback で検出するアプリレベルのエラー (log entry の整合性
  チェック失敗など) を leader に伝えたい場合は、その時点で `receive_handler_with_ack`
  への切替を検討する。

この段階の完了条件: RDMA log channel について TCP socket での ACK 送信が発生せず、
lib の自動 ACK によって leader 側 `flush()` が完了する。TCP replication の ACK 経路は
従来通り動作している。

### 5. テスト整備

- 単体テスト:
  - 新 I/F に対する null/mock 実装のシグネチャ追従を確認する。
  - SETUP/TRANSFER 遷移、finalize 後の登録拒否を確認する。
- 結合テスト:
  - RDMA replication で flush 完了まで TCP ACK に依存しないこと。
  - duplicate / stale / gap 時の挙動 (lib の failure ACK 経路) が確認できること。
  - 既存 TCP replication テストに退行がないこと。
- 既存の `set_rdma_ack_fd_for_test` 依存テストは置き換えまたは削除する。
- `receive_handler_with_ack` への切替を後段で行う場合は、その時点で ACK body が
  送信側に届くことを確認するテストを追加する。

### 6. RDMA replication における `log_channel` 責務整理 (本 TODO ではクローズ)

§1〜§5 完了後に再検討した結果、本 TODO の範囲ではクローズし、後日「将来の cleanup」
として別 issue で扱う。理由は以下の通り。

#### 6a. socket session を使った ACK 送受信コードの削除 — 残作業なし

- 当初の対象だった「`channel_handler_base::send_ack()` の RDMA log channel 経路から
  呼ばれる部分」は、§4 (commit 54dc64b) の段階で `ack_enabled=false` gate により
  既に呼ばれない状態。`message_log_entries::post_receive()` の per-message ACK は
  RDMA path では発行されない。
- master 側 `wait_for_replica_ack()` も `has_rdma_send_stream()` 分岐で TCP path 専用
  となっており、RDMA path には残骸なし。
- 現状 `send_ack()` を呼んでいるのは `log_channel_handler::send_initial_ack()` のみで、
  これは LOG_CHANNEL_CREATE handshake 用 (§6b の範疇)。本節で削除すべきコードはない。

#### 6b. RDMA ACK モードでは log channel そのものを作らない — 動作上は不要

- 現状 RDMA mode でも LOG_CHANNEL_CREATE 由来の TCP socket session が 512 本作られ、
  COMMON_ACK を返した後 idle hold される (使われない)。これを skip すれば idle socket
  と thread を削減できる。
- ただし scenario_test (rdma_1) を含む既存テストは現状の構成で動作しており、機能要件
  としては不要。skip するには master 側 `create_log_channel_connector` の RDMA 分岐、
  replica 側での `log_channel_handler` の bulk pre-create 機構、TCP/RDMA 経路の分岐
  追加など、影響範囲が広い変更になる。
- 本 TODO の主目的 (RDMA ACK over RDMA) は §1〜§5 で達成済み。§6b は別 issue で扱う。

## 実装順

§1 → §2 → §3 → §4 → §5 の順で完了。§6 は本 TODO ではクローズ (上記理由)。

## 注意点

- rdma-comm-lib 側は master 未マージのため、API がまだ細部で動く可能性がある。
  特に `finalize_channel_setup_with_sender` / `finalize_channel_setup` 周辺と
  buffer の 2x 確保責務 (内部 vs 呼び出し側) は実 I/F を確認しながら進める。
- `channel_handler_base::send_ack()` は control channel の初期応答にも使われている。
  全面置換すると責務を壊しやすいので、log channel 経路だけ別に分離してから縮小する。
- エラー時に TCP `COMMON_ERROR` を返す制御系と、RDMA データ面の ACK / failure ACK を
  混同しない。
- channel_id が決定論的 0..N-1 になったため、log channel 数 N を初期化前に確定させる
  必要がある。N は定数 512 とし、初期化時に 512 個の channel を pre-create する。
  これを超える `LOG_CHANNEL_CREATE` 要求が来た場合はエラーにする。これにより
  「LOG_CHANNEL_CREATE の到着で動的に増える」構造が不要になる。
- buffer の 2x 確保は rdma-comm-lib が内部で行うため limestone 側はサイズ計算を意識
  する必要がないが、実メモリ使用量が指定 `region_size_bytes` の 2 倍になる点だけは
  押さえておく。
- RDMA replication における `log_channel` の不要化 (§6b) は影響範囲が広い。
  本 TODO ではクローズし、別 issue で扱う。
- sequence の wrap-around と複数フレームをまたぐ 1 メッセージの ACK 粒度については、
  rdma-comm-lib 側の sequence/channel 取り扱いに従う前提で進める。

## 完了条件

- RDMA log channel の ACK が TCP socket を経由せず RDMA で完結している。
- `flush()` 相当の完了待ちが RDMA ACK によって成立する。
- SETUP → finalize → TRANSFER の初期化シーケンスが確立しており、finalize 後の
  channel 追加登録が拒否される。
- 既存の TCP replication テストが通る。
- RDMA replication 系テストが通り、duplicate / stale / gap の挙動が確認できている。
- `channel_handler_base::send_ack()` の log channel データ面 (per-message ACK) からの
  呼び出しが RDMA path で消えている (control channel handshake および LOG_CHANNEL_CREATE
  handshake からの呼び出しは残る; 後者の撤去は §6b として別 issue 化)。

## 積み残し (本 TODO の範囲外・将来の改善要望)

### RDMA mode での log channel TCP socket session 撤去 (旧 §6b)

RDMA mode でも `LOG_CHANNEL_CREATE` 由来の TCP socket が 512 本作成され、handshake 後
idle hold される。RDMA ACK over RDMA が完成した今、これらは機能的に不要。撤去には
master 側 `create_log_channel_connector` の RDMA 分岐、replica 側 `log_channel_handler`
の bulk pre-create、TCP/RDMA 経路の分岐などが必要で、影響範囲が広い。

優先度: 低 (機能性ではなくリソース効率の問題)。本 TODO とは別 issue で扱う。

### rdma-comm-lib への仕様改善要望: 片方向 RDMA Write 用途での buffer 浪費の解消

`feat/rdma-ack-over-rdma` 現仕様では、各 side が `rdma_sender` と `rdma_receiver` を
別 instance として new し、それぞれが独立に 2x buffer を確保する。`blob_relay` のよう
に双方向 RDMA Write を行う用途では両半分とも有効活用できるが、limestone のように
片方向 RDMA Write しか使わない用途では、各 instance の半分が常に未使用となる
(side あたり実 4x、有効 2x)。

要望内容: 片方向 RDMA Write 用途で 1 side = 1 instance / 1 buffer (2x) で済むよう、
sender に「ACK 受信領域」、receiver に「ACK 送信領域」を同一 buffer 内へ集約できる
モードを設ける (もしくは sender/receiver を統合した新 facade を提供する)。

優先度: 低 (機能性ではなくメモリ効率の問題、機能完成後に lib 側で検討)。
本 TODO の §2〜§6 の範囲では現仕様を受け入れて進める。
