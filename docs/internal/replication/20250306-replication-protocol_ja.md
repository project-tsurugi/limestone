# レプリケーション最小構成の通信プロトコル

## この文書について

* この文書は、Tsurugi のレプリケーション最小構成 (仮コード: `REP-0.0`) における、マスター・レプリカ間の具体的な通信内容について記述する

## デザインコンセプト

* すべての通信は、マスターからレプリカへのリクエスト・レスポンス形式で行う
  * つまり、すべての通信の起点はマスターからであり、レプリカはマスターからのリクエストに対し、単一のレスポンスを返す
* すべての通信は、

## 表記について

* この文書では、プロトコルの表記に C の構造体に似た記法を用いる
* フィールドのデータ型は以下の通り
  * `u<size>` - `<size>` バイトの符号なし整数
  * `s<size>` - `<size>` バイトの符号付き整数
  * `<struct-name>` - `<struct-name>` の構造体型
  * `<type>[<size>]` - `<type>` 型、要素数 `<size>` の配列
    * `<size>` には符号なし整数型のフィールド名が指定される場合があり、その場合はフィールドに格納された数値を長さにとる
* フィールドの定義は以下の通り
  * `<type> <name> ;`
    * `<type>` 型のフィールド `<name>` を定義する
  * `<type> <name> = <integer>;`
    * `<type>` 型 (整数型) のフィールド `<name>` を定義し、固定値として `<integer>` を設定する
* 以下の記法で構造体に別名を付けることができる
  * `struct <struct-name-1> = <struct-name-2>;`
    * `struct-name-1` の構造体を定義し、その構造は `struct-name-2` の構造と同等とする
* 例:

  ```c
  // 文字列 (または、バイト列) 型
  struct string {
      // 文字列のバイト数
      u32 size;
      // 文字列のバイト列
      u8[size] data;
  }
  ```

## プロトコル

### 共通データ構造

```c
struct string {
    u32 size;
    u8[size] data;
}
```

```c
struct ack {
    u1 response_type = RESPONSE_TYPE_ACK;
}
```

```c
struct error {
    u1 response_type = RESPONSE_TYPE_ERROR;
    u2 error_code;
    string message;
}
```

### TCP ソケットのステート

| ステート名 | 説明 |
|:--|:--|
| `CONNECTED` | TCP ソケットに接続した状態 |
| `DISCONNECTED` | TCP ソケットを切断した状態 |
| `CONTROL` | コントロールチャネルとして初期化された状態 |
| `LOG` | ログチャネルとして初期化された状態 |

### プロトコル：レプリケーションセッション開始

* 概要
  * レプリケーションセッションを開始し、コントロールチャネルを確立する
* 開始ステート
  * `CONNECTED`
* 終端ステート
  * `CONTROL`

マスターからの送信:

```c
struct request.session_begin {
    u1 connection_type = CONNECTION_TYPE_CONTROL_CHANNEL;
    u8 protocol_version;
    string configuration_id;
    u8 epoch_number;
}
```

| フィールド名 | 説明 |
|:--|:--|
| `protocol_version` | プロトコルバージョン番号 |
| `configuration_id` | マスターを識別するための文字列 |
| `epoch_number` | 最終エポック番号 |

レプリカからの返信:

```c
// 接続成功時
struct response.session_begin_ok {
    u1 response_type = RESPONSE_TYPE_ACK;
    string session_secret;
}

// 接続失敗時
struct response.session_begin_error = error;
```

| フィールド名 | 説明 |
|:--|:--|
| `session_secret` | セッションを識別するための文字列 |

### プロトコル：レプリケーションセッション終了

* 概要
  * コントロールチャネルを破棄し、レプリケーションセッションを終了する
* 開始ステート
  * `CONTROL`
* 終端ステート
  * `DISCONNECTED`

マスターからの送信:

```c
struct request.session_end {
    u1 command_type = CONTROL_COMMAND_TYPE_DISPOSE;
}
```

レプリカからの返信:

```c
// 正常終了時には、何も返さずに接続自体が切れる

// エラー発生時
struct response.session_end_error = error;
```

### プロトコル：グループコミット

* 概要
  * 特定のエポック番号のグループコミットを完了させる
* 開始ステート
  * `CONTROL`
* 終端ステート
  * `CONTROL`

マスターからの送信:

```c
struct request.group_commit {
    u1 command_type = CONTROL_COMMAND_TYPE_GROUP_COMMIT;
    u8 epoch_number;
}
```

| フィールド名 | 説明 |
|:--|:--|
| `epoch_number` | グループコミット対象のエポック番号 |

レプリカからの返信:

```c
// 成功時
struct response.group_commit_ok = ack;

// 失敗時
struct response.group_commit_error = error;
```

### プロトコル：セーフスナップショット切り替え

TBD (このフェーズでは発行しない)

### プロトコル：GC境界切り替え

* 概要
  * ログエントリの GC 境界を設定する
* 開始ステート
  * `CONTROL`
* 終端ステート
  * `CONTROL`

マスターからの送信:

```c
struct request.gc_boundary_switch {
    u1 command_type = CONTROL_COMMAND_TYPE_GC_BOUNDARY_SWITCH;
    u16 write_version;
}
```

| フィールド名 | 説明 |
|:--|:--|
| `write_version` | GC 境界として設定するライトバージョン番号 |

レプリカからの返信:

```c
// 成功時
struct response.gc_boundary_switch_ok = ack;

// 失敗時
struct response.gc_boundary_switch_error = error;
```

### プロトコル：ログチャネル作成

* 概要
  * ログチャネルを確立する
* 開始ステート
  * `CONNECTED`
* 終端ステート
  * `LOG`

マスターからの送信:

```c
struct request.log_channel_create {
    u1 connection_type = CONNECTION_TYPE_LOG_CHANNEL;
    string secret;
}
```

| フィールド名 | 説明 |
|:--|:--|
| `secret` | セッションを識別するための文字列 |

レプリカからの返信:

```c
// 接続成功時
struct response.log_channel_create_ok = ack;

// 接続失敗時
struct response.log_channel_create_error = error;
```

### プロトコル：ログチャネル破棄

* 概要
  * ログチャネルを破棄する
* 開始ステート
  * `LOG`
* 終端ステート
  * `DISCONNECTED`

マスターからの送信:

```c
struct request.log_channel_dispose {
    u1 command_type = LOG_COMMAND_TYPE_DISPOSE;
}
```

レプリカからの返信:

```c
// 成功時には、何も返さずに接続自体が切れる

// 失敗時
struct response.log_channel_dispose_error = error;
```

### プロトコル：ログエントリ送付

* 概要
  * ログエントリを送付する
* 開始ステート
  * `LOG`
* 終端ステート
  * `LOG`

マスターからの送信:

```c
struct request.log_entry_write {
    u1 command_type = LOG_COMMAND_TYPE_WRITE;
    u8 epoch_number;
    u4 entry_list_size;
    log_entry[entry_list_size] entry_list;
    u1 operation_flag_set;
}
```

| フィールド名 | 説明 |
|:--|:--|
| `epoch_number` | エントリのエポック番号 |
| `entry_list_size` | エントリリストの要素数 |
| `entry_list` | エントリリスト (`struct log_entry` 参照) |
| `operation_flag_set` | 操作フラグ |

操作フラグの一覧:

| フラグ名 | 値 | 説明 |
|:---|:--:|:--|
| `LOG_OPERATION_SESSION_BEGIN` | 0x01 | 永続化セッションを開始する |
| `LOG_OPERATION_SESSION_END` | 0x02 | 永続化セッションを終了する |
| `LOG_OPERATION_FLUSH` | 0x04 | ログチャンネルをフラッシュする |

```c
struct log_entry {
    u1 operation_type;
    u16 write_version;
    u8 storage_id;
    string key;
    string value;
    u6 blob_list_size;
    blob_entry[blob_list_size] blob_list;
}
```

| フィールド名 | 説明 |
|:--|:--|
| `operation_type` | エントリの操作タイプ |
| `write_version` | エントリのライトバージョン番号 |
| `storage_id` | ストレージ ID |
| `key` | キーバイト列 |
| `value` | 値バイト列 |
| `blob_list_size` | BLOB エントリリストの要素数 |
| `blob_list` | BLOB エントリリスト (`struct blob_entry` 参照) |

```c
struct blob_entry {
    u8 blob_id;
    u8 data_size;
    u1[data_size] data;
}
```

| フィールド名 | 説明 |
|:--|:--|
| `blob_id` | BLOB ID |
| `data_size` | データバイト数 |
| `data` | データバイト列 |

レプリカからの返信:

```c
struct response.log_entry_write_ok = ack;

struct response.log_entry_write_error = error;
```

### プロトコル：ログチャネル書き出し

* 概要
  * ログチャネルの内容をディスクに書き出す (fsync)
* 開始ステート
  * `LOG`
* 終端ステート
  * `LOG`

マスターからの送信:

```c
struct request.log_channel_flush {
    u1 command_type = LOG_COMMAND_TYPE_FLUSH;
}
```

レプリカからの返信:

```c
// 成功時
struct response.log_channel_flush_ok = ack;

// 失敗時
struct response.log_channel_flush_error = error;
```
