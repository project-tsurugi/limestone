# RDMA BLOB レプリケーション修正 TODO

## 目的

RDMA BLOB レプリケーションで、大きな BLOB を sender / receiver のどちらでも
全量メモリに載せず、streaming で転送できるように修正する。

現在失敗している regression test は次のとおり。

```sh
./build-rdma-rep-tests/test/limestone-test \
  --gtest_filter=log_channel_handler_test.handle_rdma_data_event_accepts_blob_split_by_rdma_socket_io
```

## 方針検討

### 受信処理方式と性能

RDMA 版を TCP 版のような stream 処理に寄せる案も検討した。
設計としてはきれいで、既存の `socket_io` / `blob_socket_io` に近づけやすい。
しかし RDMA write size は 4 KB から 64 KB 程度と小さいため、callback ごとに
worker thread を起こす素朴な stream 実装では context switch の回数が多くなり、
性能面で不利になる可能性が高い。

一方、callback 内で BLOB chunk を file に直接 `write()` する方式は、
余分な user-space buffer copy と payload ごとの thread wakeup を避けられる。
通常の buffered file write は多くの場合 page cache への copy なので、
「callback 内で I/O するから必ず遅い」とは言えない。

ただし、callback 内 write には次のリスクがある。

- page cache / filesystem / memory pressure によって `write()` が詰まる可能性がある。
- callback が RDMA receive progress thread 上で長く止まると、RDMA 側の進行に影響する可能性がある。
- 小さい chunk が大量に来るため、syscall 回数は多くなる。

現時点の第一候補は次の方針とする。

```text
RDMA BLOB 専用 protocol
+ callback 内 state machine
+ callback 内 buffered file write
```

callback 内では `fsync()` / `fdatasync()` のような同期 I/O は行わない。
将来 callback blocking が問題になった場合に備えて、BLOB 受信処理は
`rdma_blob_receiver` のような独立したクラス境界に閉じ込め、
内部実装を batched writer thread に差し替えられるようにする。

### Flush と ACK の分離

TCP 版と RDMA 版では ACK の見え方が異なる。

TCP 版では、transport ACK は TCP protocol ACK として OS の TCP stack が処理するため、
limestone からは見えない。limestone が明示的に扱う ACK は `COMMON_ACK` だけである。

RDMA 版では、ACK は rdma-comm-lib の内部メッセージとして明示的に扱われる。
この ACK は RDMA frame の `sequence_number` 単位で返され、送信側 `flush()` は
pending ACK が 0 になるまで待つ。

重要なのは、rdma-comm-lib の現行 ACK は単なる「sender buffer 解放通知」ではなく、
「受信側ユーザハンドラの処理完了通知」として定義されていること。
受信側は RDMA 通知をデコードし、正常イベントまたはエラーイベントをユーザハンドラへ渡し、
そのハンドラが return した後に ACK を返す。
したがって `flush()` の成功は、

- 送信済み RDMA frame すべてについて ACK が返ったこと
- その ACK は、受信側ユーザハンドラの処理完了後に送られたこと

を意味する。

一方で、この ACK は durable ACK ではない。
`fsync()` / `fdatasync()` の完了やアプリケーションレベルの成功は `flush()` だけでは保証されない。
アプリケーションレベルの成否は、必要に応じて ACK body で通知し、
`flush()` 後に `take_ack_body()` で回収する設計である。

BLOB 対応では、巨大 BLOB を多数の RDMA frame に分割するため、
どの単位で「受信側ユーザハンドラの処理完了」とみなすかが問題になる。
`BLOB_DATA` frame ごとに file write が終わればすぐ ACK を返すのか、
あるいは BLOB 全体や WAL apply 完了まで ACK を遅延させるのかで、`flush()` の意味が変わる。

現時点では、次の順序で扱う前提で検討する。

```text
RDMA frame callback
-> protocol / sequence / state check
-> BLOB chunk を buffered file write
-> write 成功
-> callback return
-> rdma-comm-lib が transport ACK を送信
-> sender 側 rdma-comm-lib が pending ACK を完了し、send buffer slot を解放
```

この方針でよいか、rdma-comm-lib の仕様変更が必要かを先に整理する。

## 作業方針

### 1. rdma-comm-lib で ACK / flush の責務を整理する

- 現行の `flush()` / ACK 仕様は維持する。
- `flush()` は、受信側ユーザハンドラが各 RDMA frame の処理を終えて return し、
  transport ACK が pending 0 になるまで待つ。
- BLOB_DATA は frame ごとに処理する。
- 受信側 callback は、受信した chunk を file に書き込んだら return する。
- callback の return 後に rdma-comm-lib が transport ACK を返す。
- sender 側は、必要な frame をすべて送信し終えてから `flush()` を呼ぶ。
- `flush()` が返った時点で、その logical unit に属する全 frame について、
  受信側 handler の処理完了まで到達したとみなす。
- BLOB 全体完了や WAL entry 処理完了を `ack_body` で通知する設計は採用しない。
- `take_ack_body()` は今回の主経路には使わない。
- limestone 側設計は、この前提の上で RDMA BLOB sub-protocol をどう切るかに集中する。

### 2. RDMA BLOB sub-protocol を設計する

- 最小限必要な RDMA BLOB frame / message 種別を決める。
- 各種別の payload format を決める。
- protocol 定義を `src/limestone/rdma/` に置くか、
  `src/limestone/replication/` に置くか決める。
- `message_log_entries` の metadata と BLOB data をどう分離するか決める。
- receiver がどの時点で WAL entry を apply するか決める。
- BLOB 転送が中断または失敗したときに、partial file をどう cleanup するか決める。

暫定設計:

```text
既存の LOG_ENTRY wire format は基本的に維持する。

message_type_id                 : uint8
epoch_id                        : uint64
entry_count                     : uint32

repeat entry_count times:
  entry_type                    : uint8
  storage_id                    : uint64
  key_length                    : uint32
  key_bytes                     : key_length bytes
  value_length                  : uint32
  value_bytes                   : value_length bytes
  write_version.major           : uint64
  write_version.minor           : uint64
  blob_count                    : uint32

その後、各 BLOB について既存どおり

  blob_id                       : uint64
  blob_size                     : uint32
  blob_bytes                    : blob_size bytes

をこの順で送る。
```

ただし RDMA 版では、`blob_bytes` を byte stream として一括で受けるのではなく、
rdma-comm-lib が 1 回の送信用に確保できたバッファサイズに応じて分割して送る。
1 回の RDMA write では、その時点で確保できた送信バッファに収まる分だけを送る。
したがって protocol 上は既存どおり `blob_id + blob_size + blob_bytes` だが、
実装上は `blob_bytes` が複数回の RDMA write に分かれることを前提にする。

分割戦略は次のとおりとする。

- BLOB の先頭 write では、可能なら `blob_id + blob_size + blob_bytes` の先頭部分を
  同じ送信バッファに載せる。
- 先頭 write に載せきれない残りの `blob_bytes` は、以後の RDMA write に分割して送る。
- 途中 write では `blob_bytes` の未送信部分のみを送る。
- sender は BLOB 全量を 1 個の送信バッファに載せない。

receiver 側では、従来の `message_log_entries::receive_body()` をそのまま使わず、
少なくとも次の 2 段階に処理を分離する。

- entry 固定部
  - `message_type_id` から各 entry の `blob_count` までを読む。
- blob 部
  - `blob_id`、`blob_size`、`blob_bytes` を読み、replica 側 BLOB file を生成する。

receiver は BLOB 全体を `std::vector` や `std::string` に集約せず、
受信した `blob_bytes` をそのまま replica 側の file に追記する。

また、BLOB 付き LOG_ENTRY の場合は、entry 固定部を送った時点で一度 RDMA write を実行する。
blob 部はその後に続けて送る。
一方、BLOB を含まない WAL entry は、従来どおり一定サイズ以上になるまで
送信を遅延させる方針を維持する。

### 3. Sender 側を実装する

- `blob_socket_io::send_blob()` と `rdma_socket_io::send_blob()` に重複している
  BLOB file の open / size check / chunk read 処理を共通化する。
- TCP 用と RDMA 用で、BLOB の wire format の意味
  (`blob_id + blob_size + blob_bytes`) は維持する。
- RDMA 用では、BLOB 付き LOG_ENTRY の送信を
  - entry 固定部の送信
  - blob 部の送信
  の 2 段階に分ける。
- BLOB を含まない RDMA replication path は変えない。
- sender 側で、BLOB 付き LOG_ENTRY が
  - entry 固定部
  - 各 BLOB の `blob_id + blob_size + blob_bytes`
  の順で送られることを確認する test を追加する。
- 100 MB から 1 GB 超の BLOB でも streaming で動作することを前提にする。

### 4. Receiver 側を実装する

- RDMA 用の `message_log_entries` 受信処理を、
  - entry 固定部
  - blob 部
  に分離する。
- BLOB chunk を replica datastore の BLOB file に直接書き込む。
- 必要な BLOB がすべて揃うまで `message_log_entries::post_receive()` を呼ばず、
  replica 側 `log_channel` に WAL entry を反映しない。
- BLOB 全体を `std::vector` や `std::string` に集約しない。
- sequence number、payload size、channel validation の既存チェックを維持する。

#### 4.1 entry という語の整理

この作業では `entry` という語が複数の意味で出てくるため、
TODO ではどちらを指しているかを明示する。

1. WAL entry type: `limestone::api::log_entry::entry_type`
   - WAL file の `log_entry` が持つ entry type。
   - 代表的な値は次のとおり。
     - `normal_entry`
     - `normal_with_blob`
     - `remove_entry`
     - `marker_begin`
     - `marker_end`
     - `marker_durable`
     - `marker_invalidated_begin`
     - `clear_storage`
     - `add_storage`
     - `remove_storage`
   - WAL file 上の binary format は `log_entry::write()` / `log_entry::read_entry_from()`
     が定義している。

2. replication entry: `message_log_entries::entry`
   - `message_log_entries` が replication 用に持つ entry。
   - `log_entry::entry_type` を field として持つが、
     replication wire format は WAL file の `log_entry::write()` と同一ではない。
   - `message_log_entries::send_body()` は、`entries_` に含まれる各 entry について
     現在は次の順でシリアライズしている。
     - `entry_type`
     - `storage_id`
     - `key`
     - `value`
     - `write_version.major`
     - `write_version.minor`
     - `blob_count`
     - `blob_id + blob_size + blob_bytes` を `blob_count` 回
   - `message_log_entries` が通常追加する entry は次。
     - `add_normal_entry()`
     - `add_normal_with_blob()`
     - `add_remove_entry()`
     - `add_clear_storage()`
     - `add_add_storage()`
     - `add_remove_storage()`
   - `message_log_entries::post_receive()` では次のように replica 側 `log_channel` に反映する。
     - `normal_entry` -> `log_channel.add_entry(...)`
     - `normal_with_blob` -> `log_channel.add_entry(..., blob_ids)`
     - `remove_entry` -> `log_channel.remove_entry(...)`
     - `clear_storage` -> `log_channel.truncate_storage(...)`
     - `add_storage` -> `log_channel.add_storage(...)`
     - `remove_storage` -> `log_channel.remove_storage(...)`
   - `marker_begin` / `marker_end` / `marker_durable` /
     `marker_invalidated_begin` は、WAL file の epoch marker や
     破損 WAL file の修復時に現れる entry type であり、
     replication の通常送信対象にはならない。
     そのため replication entry としては扱わず、
     `post_receive()` では invalid entry type として扱われる。
   - replication の session begin / session end / flush は entry ではなく、
     `message_log_entries` 末尾の `operation_flags` で表現される。

以降、このセクションで単に `entry` と書く場合は、
原則として replication message の `message_log_entries::entry` を指す。
WAL file の entry type 全体を指す場合は `log_entry::entry_type` と明記する。

#### 4.2 現行の RDMA 受信フロー

```text
[現行-1] rdma-comm-lib receive callback
  - rdma-comm-lib が RDMA write で届いた 1 frame を受信し、
    limestone 側 callback を呼ぶ。
  - この時点のデータ形状は rdma_receive_event。

[現行-2] replica_server::on_rdma_receive()
  - rdma_receive_event が data event か error event かを振り分ける。
  - data event の場合だけ次へ進む。

[現行-3] replica_server::handle_rdma_data_event()
  - frame header の channel_id を見て、対応する log_channel_handler を探す。
  - channel_id が不正、または handler 未登録ならここで捨てる。

[現行-4] log_channel_handler::handle_rdma_data_event()
  - frame version、payload_size、sequence_number を検査する。
  - duplicate / stale / gap を検出する。
  - 検査を通った frame を pending_rdma_frames_ に蓄積する。

[現行-5] log_channel_handler::process_pending_rdma_messages_locked()
  - pending_rdma_frames_ から、1 logical RDMA payload に属する frame 群を探す。
  - rdma_frame_flag_partial_payload が付いている frame は
    「まだ payload が続く」とみなし、non-partial frame が来るまで待つ。
  - complete した frame 群を aggregated vector に連結する。
  - この時点で、分割 frame は 1 個の連続 byte 列に戻される。

[現行-6] log_channel_handler::process_rdma_message_locked()
  - aggregated vector を std::string payload_string へコピーする。
  - payload_string を入力にして blob_socket_io を作る。
  - 以後は RDMA 専用処理ではなく、TCP stream と同じ socket_io 風の
    deserialize 経路に乗せている。

[現行-7] replication_message::receive()
  - blob_socket_io から message_type_id を 1 byte 読む。
  - message_type_id に対応する replication_message 派生型を factory で作る。
  - LOG_ENTRY の場合は message_log_entries を作る。
  - 作成した message に対して receive_body() を呼ぶ。

[現行-8] message_log_entries::receive_body()
  - epoch_id、entry_count、各 entry の固定部を読む。
  - 各 entry について blob_count を読み、
    blob_count 回だけ io.receive_blob() を呼ぶ。
  - 最後に operation_flags を読む。

[現行-9] blob_socket_io::receive_blob()
  - blob_id、blob_size を読む。
  - blob_size byte の blob_bytes を同じ入力 stream から読み続ける。
  - 読んだ blob_bytes を replica datastore の BLOB file に書き込む。
  - blob 全体を読み終えるまで return しない。

[現行-10] message_log_entries::post_receive()
  - receive_body() が完了した message_log_entries を log_channel に反映する。
  - normal_with_blob の場合は、すでに BLOB file が存在する前提で
    log_channel.add_entry(..., blob_ids) を呼ぶ。
```

#### 4.3 現行フローの問題点

上記の現行フローは TCP stream 用の `blob_socket_io` / `receive_body()` を
RDMA payload に再利用しているため、小さい BLOB では動く。
しかし RDMA BLOB streaming では次の理由で不適切。

- [現行-5] `process_pending_rdma_messages_locked()` が BLOB payload を含む frame を
  `aggregated` に連結するため、巨大 BLOB 全体をメモリに持つ。
- [現行-6] `process_rdma_message_locked()` が `std::string payload_string` を作るため、
  さらに copy が発生する。
- [現行-8] `message_log_entries::receive_body()` は [現行-9] `receive_blob()` まで一気に読むため、
  entry 固定部と BLOB data が別 RDMA write に分かれる新 sender と合わない。
- BLOB data が後続 frame に分割された場合、従来の [現行-9] `receive_blob()` は
  その場で必要 byte 数を読み切れず EOF とみなす。

そのため Receiver 側実装では、上記のうち
[現行-4] `log_channel_handler::handle_rdma_data_event()` までは基本的に維持し、
その後の

```text
[現行-5] pending_rdma_frames_ 集約
  -> [現行-6] process_rdma_message_locked()
  -> [現行-6] blob_socket_io
  -> [現行-7] replication_message::receive()
  -> [現行-8] message_log_entries::receive_body()
```

の流れを RDMA streaming receiver に置き換える。

#### 4.4 新しい RDMA 受信フロー

```text
[新-1] rdma-comm-lib receive callback
  - rdma-comm-lib が RDMA write で届いた 1 frame を受信し、
    limestone 側 callback を呼ぶ。
  - この時点のデータ形状は rdma_receive_event。

[新-2] replica_server::on_rdma_receive()
  - rdma_receive_event が data event か error event かを振り分ける。
  - data event の場合だけ次へ進む。

[新-3] replica_server::handle_rdma_data_event()
  - frame header の channel_id を見て、対応する log_channel_handler を探す。
  - channel_id が不正、または handler 未登録ならここで捨てる。

[新-4] log_channel_handler::handle_rdma_data_event()
  - frame version、payload_size、sequence_number を検査する。
  - duplicate / stale / gap を検出する。
  - 検査を通った frame payload を RDMA streaming receiver に渡す。
  - ここでは BLOB payload を aggregated vector に連結しない。

[新-5] RDMA streaming receiver
  - 受信状態を持ちながら、渡された frame payload を先頭から順番に消費する。
  - frame payload を std::string にコピーせず、byte 列として扱う。
  - 1 frame の中に複数 message の一部または全部が含まれる場合も、
    状態を進められるところまで進める。

[新-6] message_type_id の読み取り
  - 現在の `replication_message::receive()` をそのまま使わず、
    receiver 状態が payload から `message_type_id` を読む。
  - RDMA log channel では LOG_ENTRY だけを受け付ける。
  - LOG_ENTRY 以外なら protocol error として処理を中断する。

[新-7] message_log_entries の BLOB 前までの読み取り
  - `epoch_id` と `entry_count` を読む。
  - `entry_count` は、この LOG_ENTRY message に含まれる
    `message_log_entries::entry` の数。
  - ここでいう entry は [4.1](#41-entry-という語の整理) の
    replication entry、つまり `message_log_entries::entry` を指す。
  - 現行の replication wire format では、`message_log_entries::entry` ごとに
    `entry_type`、`storage_id`、`key`、`value`、
    `write_version.major`、`write_version.minor`、`blob_count` の順で
    シリアライズされている。
  - ただし各 field の意味は `entry_type` によって異なる。
    例えば BLOB を持たない entry では `blob_count` は 0 であり、
    `normal_with_blob` の場合だけ後続に BLOB 本体が続く。
  - ここでは wire format 上 BLOB 本体より前に置かれている部分だけを読む。
    この時点では `message_log_entries::post_receive()` を呼ばず、
    BLOB 付きでない entry も含めて、まだ replica 側 `log_channel` へ反映しない。

[新-8] BLOB header の読み取り
  - BLOB 付き entry では、各 BLOB について `blob_id` と `blob_size` を読む。
  - この時点では `blob_id` に対応する replica datastore 側の BLOB file は開かない。
    1 つの entry に大量の BLOB がある場合でも未書き込み file handle を抱えないように、
    file はその BLOB の最初の `blob_bytes` を書き込むタイミングで開く。
  - `blob_size` を残り byte 数として receiver 状態に保持する。

[新-9] BLOB bytes の file 書き込み
  - 後続 payload から読める分だけ `blob_bytes` を取り出し、
    対象 BLOB file に書き込む。
  - 対象 BLOB file がまだ開かれていなければ、この最初の書き込み直前に開く。
  - 1 frame で BLOB 全体を読み切れない場合は、残り byte 数を保持して
    次の frame を待つ。
  - `blob_size` byte を書き終えたら file を close し、
    次の BLOB または次の entry の処理へ進む。
  - BLOB 全体を `std::vector` や `std::string` に集約しない。

[新-10] operation_flags の読み取り
  - 全 entry と全 BLOB を読み終えた後で `operation_flags` を読む。
  - ここまで到達したら `message_log_entries` の deserialize 完了とみなす。

[新-11] message_log_entries::post_receive()
  - 完成した `message_log_entries` を log_channel に反映する。
  - normal_with_blob の場合は、すでに BLOB file が作成済みであることを前提に
    log_channel.add_entry(..., blob_ids) を呼ぶ。
```

#### 4.5 実装順序

実装は、各番号ごとに build / test / commit できる粒度に分けて進める。
既存 TCP 経路と、既存 RDMA 経路の validation / channel dispatch は、
切り替え step までは挙動を変えない。

1. RDMA streaming receiver 用の entry 単位 incremental parser の土台を追加する。

   既存 TCP 経路の `message_log_entries::receive_body()` は変更しない。
   `receive_body()` は、現在の TCP / socket stream 用 wire format を一括で読む責務に残す。
   RDMA 用の途中状態は `message_log_entries` に持たせず、RDMA streaming receiver 側に持たせる。

   現行 replication wire format では、BLOB data は message 末尾にまとめて置かれるのではなく、
   BLOB を持つ entry の直後に置かれる。

   ```text
   entry fixed fields
   blob_count
   blob_id + blob_size + blob_bytes
   next entry fixed fields
   ...
   operation_flags
   ```

   そのため、「全 entry の固定部を先に読んでから全 BLOB を読む」形の
   `message_log_entries` API は追加しない。
   RDMA receiver は、entry ごとに固定部、BLOB header、BLOB bytes を順番に消費し、
   BLOB file を作成し終えたあとで完成済み entry として `message_log_entries` に追加する。

   - message body 先頭の `epoch_id` と `entry_count` を incremental に読む状態を用意する。
   - 1 つの `message_log_entries::entry` について、
     `entry_type`、`storage_id`、`key`、`value`、
     `write_version.major`、`write_version.minor`、`blob_count` までを
     incremental に読む状態を用意する。
   - BLOB なし entry では、その時点で `message_log_entries` に entry を追加できることを確認する。
   - BLOB 付き entry では、`blob_count` と BLOB 受信予定情報を receiver 状態に保持し、
     entry は BLOB file の書き込み完了後に `message_log_entries` へ追加する。
   - `operation_flags` は全 entry と全 BLOB を読み終えたあとに読む。
   - TCP 既存経路の `receive_body()` は壊さない。

2. BLOB header / BLOB bytes 受信用の状態と file 書き込み処理を追加する。

   [新-8] / [新-9] に相当する処理を、既存 RDMA 経路に接続しない部品として追加する。
   この step では、byte 列を分割して渡しても BLOB を最後まで復元できることを
   単体テストで確認する。

   - BLOB ごとに `blob_id`、`blob_size`、残り byte 数を保持する。
   - BLOB header 読み取り時点では replica datastore 側の BLOB file を開かない。
   - その BLOB の最初の `blob_bytes` を書き込む直前に file を開く。
   - `blob_size` byte を書き終えたら file を close する。
   - BLOB 全体を `std::vector` や `std::string` に集約しない。

3. RDMA streaming receiver クラスを追加する。

   [新-5] / [新-6] / [新-7] / [新-8] / [新-9] / [新-10] をまとめる
   状態クラスを追加する。
   ただし、この step ではまだ [現行-5] / [現行-6] の既存 RDMA 経路には接続しない。

   状態クラスは少なくとも次を保持する。

   - 現在受信中の `message_log_entries`
   - message type を読み終えたかどうか
   - `epoch_id` / `entry_count` を読み終えたかどうか
   - 現在処理中の entry index
   - 現在処理中の blob index
   - `blob_id`
   - `blob_size`
   - BLOB の残り byte 数
   - 書き込み中の場合だけ開いている replica BLOB file handle
   - `operation_flags` を読み終えたかどうか

   この receiver は、渡された byte 列内で message が完成するたびに
   完成した `message_log_entries` を取り出せるようにする。
   完成した `message_log_entries` は `receive_body()` を通さず、
   receiver が `add_normal_entry()` / `add_normal_with_blob()` などを使って直接構築する。
   同じ byte 列に未消費 byte が残っている場合は、その byte 列を次の message の
   先頭として続けて処理できるようにする。
   ここでは BLOB なし message、BLOB 付き message、分割 BLOB、複数 message 同梱を
   単体テストする。

4. RDMA 受信経路を streaming receiver に切り替える。

   [現行-5] `process_pending_rdma_messages_locked()` と
   [現行-6] `process_rdma_message_locked()` の
   `aggregated vector -> std::string payload_string -> blob_socket_io` 経路をやめ、
   [新-4] で validation 済みの frame payload を到着順に [新-5] へ渡す。

   `rdma_frame_flag_partial_payload` は frame payload が次 frame に継続するかどうかの
   判断に使うが、message 完了判定そのものは受信状態クラスの deserialize 状態で行う。
   BLOB data は frame ごとに file へ書き込む。
   receiver が完成した `message_log_entries` を返した時点で、
   現在と同じ `log_channel_handler_resources` を使って
   [新-11] `message_log_entries::post_receive()` を呼ぶ。

   `sequence_number`、`payload_size`、version mismatch、
   out-of-order / duplicate frame の扱いは現在の
   [現行-4] / [新-4] `handle_rdma_data_event()` 側の責務として残す。
   変更対象は validation 後の payload 処理であり、
   channel dispatch や sequence check の意味は変えない。

5. End-to-end regression test を通す。

   RDMA BLOB streaming の統合動作を確認する。
   必要なら、BLOB 付き LOG_ENTRY の送信順序変更に合わせて
   failing regression test を調整する。
   具体的な確認項目は [5. End-to-end regression test を通す](#5-end-to-end-regression-test-を通す)
   に従う。

6. 後始末を行う。

   streaming receiver への切り替え後に不要になった RDMA 側の
   `blob_socket_io` 経路、helper、コメントを整理する。
   ただし TCP 既存経路で使っている `blob_socket_io` / `receive_body()` は削除しない。

### 5. End-to-end regression test を通す

- 必要なら、BLOB 付き LOG_ENTRY の送信順序変更に合わせて
  failing regression test を調整する。
- 分割された RDMA BLOB payload が正常に処理されることを確認する。
- replica 側の BLOB file が作成され、内容が sender 側と一致することを確認する。
- BLOB 転送完了前に WAL entry が書かれないことを確認する。

### 6. 異常系 test を追加する

- BLOB chunk 欠落。
- BLOB size 不一致。
- 期待する blob byte 数を満たす前の切断または abort。
- duplicate frame または out-of-order frame。
- BLOB 受信中の transfer abort または connection close。
- partial BLOB file の cleanup。
- 1 つの log entry に複数 BLOB が含まれるケース。
- 1 session に複数の BLOB log entry が含まれるケース。


## 積み残し事項

### I/O クラスの命名と責務整理

- `socket_io` は現状では socket 専用 I/O というより、
  replication message の serialize / deserialize 基盤として使われている。
- `blob_socket_io` や `rdma_socket_io` との関係を考えると、
  将来的には `replication_io` のような名前と責務に整理し直す余地がある。
- `rdma_socket_io` という名前も既存の `socket_io` に引きずられた命名であり、
  上記の整理と合わせて見直す。
- ただしこれは今回の RDMA BLOB 修正とは独立したリファクタリングであり、
  不具合修正と混ぜずに別タスクとして扱う。

### primitive wire codec の切り出し

- `socket_io` の `send_uint32()` / `send_uint64()` などは、
  I/O と wire format encoding の責務を同時に持っている。
- `rdma_socket_io` でも BLOB header を byte 列へ変換する必要があり、
  現状は一時的な `socket_io` を作って既存 encoding を再利用している。
- あるべき形としては、整数型や文字列型を replication wire format の
  byte 列へ変換する helper / codec を切り出し、
  `socket_io` と RDMA 側で共有する。
- ただし今回の修正では性能影響が小さく、変更範囲も広がるため、
  別タスクとして扱う。

### LOG_ENTRY wire format codec の切り出し

- RDMA streaming receiver 実装により、LOG_ENTRY message body の wire format に関する知識が
  `message_log_entries::send_body()` / `message_log_entries::receive_body()` と
  RDMA 側 parser に重複する。
- 重複対象は、`epoch_id`、`entry_count`、entry fields の順序、
  `blob_count`、`operation_flags` などである。
- 一連の RDMA BLOB streaming 対応が完了した後で、
  `message_log_entries_wire_codec` のような共通 helper へ整理する。
- ただし現段階で抽象化すると 4.5.2 以降の実装前に設計が先行しすぎるため、
  まずは streaming receiver の動作を完成させ、その後のリファクタリングとして扱う。

### RDMA sender 初期化テストの不安定性調査

- `initialize_rdma_sender_success_sets_sender` と
  `shutdown_rdma_sender_after_initialize_clears_sender` が、全テスト実行時に
  確率的に失敗することがある。
- IDE から単独実行する限りは安定して成功しているため、実行順序、
  vendor RDMA mock の共有状態、receiver 側準備状態、環境変数、または
  前後テストの cleanup との相互作用を疑う。
- 失敗時には sender 初期化で vendor mock 側が receiver 準備不足を示すことがある。
  ただし再現条件が実行環境に依存しているため、今回の RDMA BLOB streaming 修正とは分けて扱う。
- 後続で、RDMA mock の初期状態リセット方法、receiver/sender mock 資源の隔離、
  およびこの 2 テストを unit test として fake sender に寄せるべきかを検討する。

### blob_send_utils のエラー処理整理

- `read_blob_chunk()` のエラー処理について、
  `LOG_AND_THROW_IO_EXCEPTION` を使うのが適切かどうか再検討する。
- 特に `fread()` 失敗時の `errno` の扱い、
  `EINTR` の再試行条件、
  `feof()` / `ferror()` の見方が妥当か確認する。
- helper 内で例外化する方針を維持するか、
  error を返す helper に寄せるかも含めて整理する。

### opened_blob_file の API / クラス設計見直し

- `blob_send_utils` は `opened_blob_file` にリネームし、file 名と class 名を合わせる。
- `open_blob_file_for_send()` は `opened_blob_file::open_for_send()` に寄せ、
  open / read / close を move-only な RAII クラスへ閉じ込める。
- 呼び出し側は `FILE*` に直接触れず、`path()` / `size()` / `read_chunk()` を使う。
- この見直しは動作変更を伴わないリファクタリングとして切り出す。

## 積み残し事項の対応方針

現時点で残っている作業は、テスト残件、BLOB 送信 helper の整理、
wire codec の共通化、I/O クラスの責務整理に分けて扱う。

### 1. TODO 6 の残り異常系 test を閉じる

次の項目は対応済み、または既存 test で確認済みである。

- BLOB chunk 欠落。
- BLOB size 不一致。
- duplicate frame または out-of-order frame。
- 1 つの log entry に複数 BLOB が含まれるケース。
- 1 session に複数の BLOB log entry が含まれるケース。

残りは次の項目である。

- 期待する blob byte 数を満たす前の切断または abort。
- BLOB 受信中の transfer abort または connection close。

`partial BLOB file の cleanup` については、cleanup しない仕様とする。
未完成 BLOB file が残っても、それを参照する WAL entry は作成されないため、
通常の BLOB GC で後から削除される。したがって test では cleanup 自体ではなく、
partial file が残っても message が完成せず、WAL entry が反映されないことを確認する。

### 2. blob_send_utils のエラー処理を整理する

次に `read_blob_chunk()` のエラー処理を確認する。

- `fread()` 失敗時の `errno` の扱い。
- `feof()` / `ferror()` の判定順序。
- `EINTR` を retry する必要があるか。
- helper 内で `LOG_AND_THROW_IO_EXCEPTION` を使い続けるか、
  error を返す helper に寄せるか。

この作業は BLOB streaming の I/O failure handling に直結するため、
codec 共通化より前に扱う。

### 3. opened_blob_file の API / クラス設計を見直す

エラー処理方針を整理した後で、`opened_blob_file` 周辺を見直す。

- `blob_send_utils` を `opened_blob_file` にリネームし、file 名と class 名を合わせる。
- `open_blob_file_for_send()` を `opened_blob_file::open_for_send()` に移し、
  open / read / close を move-only RAII クラスへ閉じ込める。
- `FILE*` accessor は公開せず、送信側は `read_chunk()` だけを使う。

これは動作変更を伴わないリファクタリングとして、エラー処理整理とは
別の commit に分ける。

### 4. primitive wire codec を切り出す

対応済み。`socket_io` の整数 encoding / decoding と RDMA 側の byte buffer
encoding が重複しないよう、`primitive_wire_codec` を切り出した。

- `uint8` / `uint16` / `uint32` / `uint64` の wire format helper を追加。
- `socket_io` の primitive send / receive は codec を利用する。
- `rdma_socket_io` の BLOB header encoding は codec を利用する。
- `rdma_log_entries_parser` の scalar decode は codec を利用する。

string length / string bytes は `uint32` length + raw bytes の組み合わせであり、
現時点では primitive codec には含めない。

### 5. LOG_ENTRY wire format codec を切り出す

`message_log_entries::send_body()` / `receive_body()` と
`rdma_log_entries_parser` に重複している LOG_ENTRY wire format の知識を整理する。

- `epoch_id`。
- `entry_count`。
- entry fields の順序。
- `blob_count`。
- `operation_flags`。

primitive wire codec を先に用意し、その上で `message_log_entries_wire_codec`
のような共通 helper へ整理する。

### 6. I/O クラスの命名と責務整理

最後に、`socket_io` / `blob_socket_io` / `rdma_socket_io` の名前と責務を整理する。

`socket_io` は現状では socket 専用 I/O ではなく、replication message の
serialize / deserialize 基盤として使われている。責務整理の影響範囲が広いため、
BLOB streaming 修正や codec 共通化とは分け、最後に別タスクとして扱う。

### 完了扱い

`RDMA sender 初期化テストの不安定性調査` は対応済みのため、
以降の積み残し対象から外す。
