#  レプリケーションのRDMA対応

このドキュメントは、レプリケーション機能のRDMA対応の作業ログです。
後から、作業内容を確認することを目的としますが、仕様書ではありません。


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

TODO: RDMAの初期化処理の実装