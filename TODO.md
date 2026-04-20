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
- 必要な BLOB がすべて揃うまで `message_log_entries` を apply しない。
- BLOB 全体を `std::vector` や `std::string` に集約しない。
- sequence number、payload size、channel validation の既存チェックを維持する。

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

## 現在の状況

- `rdma-comm-lib` 側に `send_with_writer()` を追加し、
  `limestone` 側の `rdma_send_stream_base` にも同等 I/F を追加した。
- sender 側では、`rdma_socket_io::send_blob()` / `send_blob_data()` を
  `send_with_writer()` ベースに切り替えた。
  - BLOB の送信バッファを rdma-comm-lib 側で確保し、
    その buffer に file から直接 read して送る構造に変更した。
- `blob_send_utils` を追加し、BLOB file の open / size check / chunk read の共通化を進めた。
- `blob_send_utils_test.*` は通過した。
- `log_channel_replication_test` は sender 側 test double を
  `send_with_writer()` 対応に更新した。
  ただし RDMA BLOB まわりの test は、追加 I/F と新しい送信形に合わせた
  調整がまだ残っている可能性がある。
- sender 側の残件として、次を行う必要がある。
  - 実装差分を確認し、変更が最小限に収まっているか見直す。
  - sender 側 test が十分か確認し、必要に応じて追加する。
  - 特に `blob_socket_io` に関する test が不足しているため、必要な test 追加を検討する。
  - `send_with_writer()` が `remaining_size` より小さい capacity を返すケースを
    sender 側 test double で再現し、複数回送信時の `remaining` 更新と分割送信を確認する。
- receiver 側 (`message_log_entries` の entry 固定部 / blob 部の分離) には未着手。
- 次回は、まず sender 側の差分確認と test 追加要否の整理を行い、
  その後 receiver 側実装に進む。

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

### blob_send_utils のエラー処理整理

- `read_blob_chunk()` のエラー処理について、
  `LOG_AND_THROW_IO_EXCEPTION` を使うのが適切かどうか再検討する。
- 特に `fread()` 失敗時の `errno` の扱い、
  `EINTR` の再試行条件、
  `feof()` / `ferror()` の見方が妥当か確認する。
- helper 内で例外化する方針を維持するか、
  error を返す helper に寄せるかも含めて整理する。
