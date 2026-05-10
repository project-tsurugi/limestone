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

- 現在の `MESSAGE_RDMA_INIT` 周辺の handshake は、replica (= 受信側) の data 受信
  buffer の DMA address を leader (= 送信側) に伝える片方向交換になっている。
  RDMA ACK では sender 側にも ACK 受信 buffer が必要となるため、
  leader → replica 方向にも DMA address を渡す双方向交換へ拡張する。
- buffer の 2x 確保 (前半 data 領域・後半 ACK 領域) は rdma-comm-lib が内部で行う。
  limestone は config の `region_size_bytes` に data 領域サイズを渡せばよく、
  サイズ計算上の追加責務はない。ただし実メモリ使用量は指定値の 2 倍になる点だけ
  押さえておく。
- `rdma_sender::initialize(remote_dma_address)` のシグネチャは現時点で 1 引数のままに
  見えるため、ACK 受信 buffer の DMA address は別経路 (`rdma_receiver::get_dma_address`
  相当の sender 側 API、または既存 `get_dma_address` の意味再定義) で取得する。
  実 I/F を確認して、limestone 側の handshake message を最小拡張で済ませる方法を選ぶ。
- `MESSAGE_RDMA_INIT` の wire format を変更する場合は、replication protocol version の
  扱い (現状の数え方、互換性破棄の方針) と整合させる。RDMA replication は実機投入前
  なので破壊的変更を許容する判断はあり得る。

この段階の完了条件: server / client それぞれが、自身の ACK 用 buffer DMA address を
取得し、相手に伝えられる経路が成立している。データ転送はまだ動かなくてよい。

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

### 6. RDMA replication における `log_channel` 責務整理

§5 のテストで RDMA log replication が新 lib の自動 ACK で正しく動くことを確認した
**後** に着手する。RDMA log channel は RDMA ACK 化により socket session 経由の ACK
送受信が不要になるため、関連コードを段階的に削除していく。

#### 6a. socket session を使った ACK 送受信コードの削除

- log channel 用 socket session の作成自体は残しつつ、その session を使って ACK の
  送受信を行っているコードのみを削除する。
- 対象: `channel_handler_base::send_ack()` の RDMA log channel 経路から呼ばれる部分、
  および leader 側で旧 TCP ACK を待っていた経路の残骸 (§4 で多くは消えている想定)。
- この時点では socket session は作成されるが「使われない」状態。TCP replication 側の
  経路は壊さない。

#### 6b. RDMA ACK モードでは log channel そのものを作らない

- RDMA replication mode では `LOG_CHANNEL_CREATE` 由来の log channel session 構築自体を
  スキップする。`log_channel_handler` の初期化、関連する server/client 側初期化手順を
  見直す。
- TCP replication 側ではこれまで通り log channel session を作る。
- 影響範囲が広いため、TCP replication が壊れないことを確認しつつ段階的に進める。

## 実装順

§1 → §2 → §3 → §4 → §5 → §6a → §6b の順で進める。各節の作業内容と完了条件は
当該節に記載のとおり。§6 は §5 のテストで RDMA log replication の動作確認が
取れてから着手する。

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
- RDMA replication における `log_channel` の不要化は、影響範囲が広い。
  責務の棚卸しと段階的削除方針を先に明確にする。
- sequence の wrap-around と複数フレームをまたぐ 1 メッセージの ACK 粒度については、
  rdma-comm-lib 側の sequence/channel 取り扱いに従う前提で進める。

## 完了条件

- RDMA log channel の ACK が TCP socket を経由せず RDMA で完結している。
- `flush()` 相当の完了待ちが RDMA ACK によって成立する。
- SETUP → finalize → TRANSFER の初期化シーケンスが確立しており、finalize 後の
  channel 追加登録が拒否される。
- 既存の TCP replication テストが通る。
- RDMA replication 系テストが通り、duplicate / stale / gap の挙動が確認できている。
- `channel_handler_base::send_ack()` の責務が control channel の handshake に限定
  されている。
