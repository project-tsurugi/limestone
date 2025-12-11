#  レプリケーションのRDMA対応

このドキュメントは、レプリケーション機能のRDMA対応の作業ログです。
後から、作業内容を確認することを目的としますが、仕様書ではありません。

## テスト

- RDMA 共通ライブラリ `rdma_comm` のモック版がリンクされているため、RDMA ハードウェアなしで gtest を実行できる。


## CMakeList.txtの変更

RDMA共通ライブラリをリンクするために、CMakeList.txtを変更しました。

## 環境変数

現在、レプリケーションの動作を制御するために以下の環境変数が使用されています。

- `TSURUGI_REPLICATION_ENDPOINT`  
  レプリカ接続先を指定する。形式は `tcp://<host>:<port>`。未設定または不正な形式の場合、レプリカは無効として扱われる。

- `REPLICATION_ASYNC_SESSION_CLOSE`  
  セッション終了時のレプリカ通知を非同期パスに切り替えるフラグ。設定するとセッション終了通知を先に送り、その後ローカルのWALクローズを行う。未設定時はWALクローズ後に通知を送る。

- `REPLICATION_ASYNC_GROUP_COMMIT`  
  グループコミット通知の送信順序を非同期パスに切り替えるフラグ。設定するとレプリカへ先に通知し、その後ローカルのepoch永続化を行う。未設定時はローカル永続化後に通知を送る。


これに、RDMA接続を指定するための新しい環境変数を追加する。

- `REPLICATION_RDMA_SLOTS`  
  4KB単位スロットの数を整数で指定。未設定ならRDMAは不使用。設定時は指定スロット数でRDMAバッファを確保して有効化する。  
  有効値は1以上かつ`int32`の上限以下。0または負の値、数値以外、`int32`の上限を超える値はエラー扱いとなり、RDMAは無効（未設定時と同じ扱い）。非常に大きな値を指定した場合、メモリ確保に失敗してエラーになる可能性がある。

### REPLICATION_RDMA_SLOTS の読み込み

- datastore_impl が起動時に `REPLICATION_RDMA_SLOTS` を読み取り、1..INT32_MAX の整数のみ有効とする。
- 不正値や未設定の場合は ERROR/INFO ログを出し、RDMAは無効扱い（レプリケーションは従来経路のみ）。
- 有効値が指定された場合、指定スロット数（4KB/slot）でRDMAバッファを確保する前提で初期化する。
- gtest で未設定/有効値/非数/ゼロ/負値/INT32_MAX超過をカバーし、期待通り無効化・有効化されることを確認済み。

## コントロールチャネルでのRDMA初期化ハンドシェイク

### 方針

- master側: `open_control_channel()` でTCP接続後に、RDMA利用有無とスロット数を送信するハンドシェイクメッセージを追加する。
- replica側: 受信した内容でRDMAを初期化し、成功ならACK、失敗ならエラー応答を返す。
- master側: ACK受信時のみRDMA初期化を進め、エラー応答時はRDMA無効で継続するか致命扱いにする方針を決める。
- 必要に応じてメッセージ種別定義やencode/decodeの追加、INFO/ERRORログを実装予定。

### メッセージの追加とプロトコルバージョンの更新

- プロトコルバージョンを`2`に更新（`replication_message.h`定数）。`SESSION_BEGIN`送受信はこのバージョンを前提にする。
- RDMA初期化要求メッセージ `RDMA_INIT`（`0x30`）を追加。ペイロードは `uint32 slot_count` のみ。`message_rdma_init` クラスでsend/receive/factory登録を実装。
- 応答は既存の `COMMON_ACK` / `COMMON_ERROR` を流用（ACKはペイロードなし、ERRORは `error_code + message`）。
- replica側 `post_receive` はスケルトン（TODO）で、後続で`control_channel_handler_resources`経由の初期化処理を実装する前提。

### レプリカとの接続時のプロトコルバージョンチェック

* レプリカ側は、バージョンチェックを行い、バージョン不一致時はエラー応答を返す。
* master側は、`SESSION_BEGIN`応答でバージョン不一致が判明した場合、FATALログを出してプロセスを終了する。

### RDMA_INIT専用のACKメッセージを追加

- RDMA_INIT完了を通知する専用ACK `RDMA_INIT_ACK` を新設し、`message_type_id` に追加する。
- ペイロードにレプリカ側が確保した `remote_dma_address`（`uint64`）を載せ、master側が `rdma_sender::initialize()` に渡せるようにする。
- エラー時は従来通り `COMMON_ERROR`（error_code + message）を使用する。
- 汎用ACK（`COMMON_ACK`）はそのままにして、RDMA_INIT専用ACKを用いることで既存メッセージへの影響を避ける。


### RDMA初期化

* replica側で、`RDMA_INIT`受信時にRDMA初期化を行う。
  * rdma_receiverクラスの初期化を行う。
  * rdma_receiverクラスは、replica_serverのフィールドに置く。
* master側は、レプリカの初期化が成功したときに、RDMA送信の初期化を行う。
  * rdma_senderクラスの初期化を行う。
  * rdma_senderクラスは、datastore_implクラスのフィールドに置き、log_channelクラスからアクセス可能にする。

#### rdma_receiverの初期化

- replica_server に rdma_receiver を process lifetime で保持する方針とし、control_channel_handler_resources 経由でアクセスする。
- RDMA_INIT の post_receive で replica_server::initialize_rdma_receiver() を呼び出し、成功時に get_rdma_dma_address() を専用 ACK (RDMA_INIT_ACK) に載せる。取得できない場合や初期化失敗時は COMMON_ERROR を返す。
- エラーコードは message_error の定数を使用し、マジックナンバーを排除した。control/log channel のバリデーションエラーを新しい定数（10番台/20番台）に置き換え、RDMA INIT 用のエラーも100番台で再定義し直した。
- rdma_config は当面、send_buffer/remote_buffer を同一設定で使用する。`region_size_bytes = slot_count * 4096`、`chunk_size_bytes = 4096`、`ring_capacity = slot_count`。completion_queue_depth は 1024 を設定し、control_channel はデフォルト（未設定）。
- TODO: 受信ハンドラ／チャネル登録（ACK用FDなど）は未実装。rdma_receiver::initialize() に渡す receive_handler の実装と、各チャネルの登録・ACK通知経路を設計した上で組み込む必要がある。

### master側のRDMA sender初期化

- コントロールチャネル確立直後に `SESSION_BEGIN` を送信し、プロトコル v2 の正常応答を確認したら直ちに `RDMA_INIT` を送る。`slot_count` は datastore 起動時に決めた値をそのまま送り、未設定なら `RDMA_INIT` は送らない。
- `RDMA_INIT_ACK` 受信時に `rdma_sender` を生成・初期化する。`rdma_config` は receiver と同一設定をベタ書きで使用する（`region_size = slot_count * 4096`、`chunk_size = 4096`、`ring_capacity = slot_count`、`cq_depth = 1024`、remote buffer は ACK の DMA アドレスを使う）。
- `rdma_sender::initialize(remote_dma_address)` が失敗した場合は RDMA 無効としてログに残し、従来経路で継続する。再初期化用の mutex/フラグは持たない。
- RDMA 初期化結果は datastore_impl などでフラグ管理し、後続のログ送信パスは `get_rdma_sender()` などで RDMA 利用可否を判定する。
- テスト観点: ACK 受信で sender 初期化成功、initialize 失敗時は RDMA 無効化で継続、ACK なし/ERROR の場合は RDMA 無効フラグのまま。



## TODO

* rdma_configに設定不要なフィールドや、設定可能でも原則デフォルト値を仕様すべきフィールドがある。整理が必要、別プロジェクトの問題なので、ここではTODOに記述するに留める。
