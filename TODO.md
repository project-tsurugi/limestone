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

- 現行の `flush()` / ACK 仕様を維持するか、変更するかを決める。
- ACK を「受信側ユーザハンドラの処理完了通知」として維持するのか、
  buffer 解放用 ACK と application completion を分離するのかを決める。
- BLOB_DATA frame ごとに ACK を返すのか、BLOB 全体または transaction 単位で ACK を返すのかを決める。
- 仕様変更する場合、`flush()` が待つ対象を transport ACK のままにするのか、
  application completion を待つ API に変えるのかを決める。
- `ack_body` を application result / error 通知に使う場合の保持方式を決める。
  - 現状は stream あたり 1 件だけ保持し、未取得の body があると後続 body が drop される。
  - logical ACK に使うなら queue 化や sequence number との対応付けが必要か検討する。
- limestone 側の RDMA wrapper に必要な API を expose する。
  - receiver 側: `receive_handler_with_ack` 相当。
  - sender 側: `take_ack_body()` または logical ACK 取得 API。
- この整理が終わるまで、limestone 側で ACK の意味を独自に増やさない。

### 2. RDMA BLOB sub-protocol を設計する

- 最小限必要な RDMA BLOB frame / message 種別を決める。
- 各種別の payload format を決める。
- protocol 定義を `src/limestone/rdma/` に置くか、
  `src/limestone/replication/` に置くか決める。
- `message_log_entries` の metadata と BLOB data をどう分離するか決める。
- receiver がどの時点で WAL entry を apply するか決める。
- BLOB 転送が中断または失敗したときに、partial file をどう cleanup するか決める。

初期案:

```text
RDMA_STREAM_BEGIN
RDMA_STREAM_BYTES
RDMA_BLOB_BEGIN
RDMA_BLOB_DATA
RDMA_BLOB_END
RDMA_STREAM_END
```

receiver は `RDMA_BLOB_DATA` を replica 側の BLOB file に直接書き込む。
BLOB 全体を `std::vector` や `std::string` に集約してはいけない。

### 3. Protocol 型とテストを追加する

- frame / message type 定義を追加する。
- encode / decode helper を追加する。
- 各 frame 種別に対する focused unit test を追加する。
- 不正な frame type と不正な payload の test を追加する。

### 4. Sender 側を実装する

- `rdma_socket_io::send_blob()` の暗黙的な byte-stream 分割をやめる。
- BLOB metadata と BLOB chunk を、明示的な RDMA BLOB protocol frame として送る。
- BLOB を含まない RDMA replication path は変えない。
- sender 側で frame の順序と chunk 境界を確認する test を追加する。
- 100 MB から 1 GB 超の BLOB でも streaming で動作することを前提にする。

### 5. Receiver 側を実装する

- `log_channel_handler` が RDMA BLOB sub-protocol を処理できるようにする。
- BLOB chunk を replica datastore の BLOB file に直接書き込む。
- 必要な BLOB がすべて揃うまで `message_log_entries` を apply しない。
- BLOB 全体を `std::vector` や `std::string` に集約しない。
- sequence number、payload size、channel validation の既存チェックを維持する。

### 6. End-to-end regression test を通す

- 必要なら、新 protocol に合わせて failing regression test を調整する。
- 分割された RDMA BLOB payload が正常に処理されることを確認する。
- replica 側の BLOB file が作成され、内容が sender 側と一致することを確認する。
- BLOB 転送完了前に WAL entry が書かれないことを確認する。

### 7. 異常系 test を追加する

- BLOB chunk 欠落。
- BLOB size 不一致。
- 予期しない `RDMA_BLOB_END`。
- duplicate frame または out-of-order frame。
- BLOB 受信中の transfer abort または connection close。
- partial BLOB file の cleanup。
- 1 つの log entry に複数 BLOB が含まれるケース。
- 1 session に複数の BLOB log entry が含まれるケース。
