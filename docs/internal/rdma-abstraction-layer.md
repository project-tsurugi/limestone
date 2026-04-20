# RDMA 抽象化レイヤー

## 概要

`src/limestone/rdma/` ディレクトリは Bridge パターンを用いて実装されており、`rdma-comm-lib` への依存を `limestone` コードベースの残りの部分から完全に隔離している。

limestone の内部コードは、このレイヤーで定義された抽象インターフェースとのみやり取りする。具体的な実装（`rdma-comm-lib` を使うものと、何もしない null 実装）はビルド時にファクトリ関数を通じて選択される。

---

## クラス階層

```
抽象インターフェース（limestone 内部、rdma-comm-lib への依存なし）
  rdma_sender_base          — センダーのライフサイクル管理 (initialize / get_send_stream / shutdown)
  rdma_send_stream_base     — チャネルごとのデータ転送 (send_bytes / send_all_bytes / send_with_writer / flush)
  rdma_receiver_base        — レシーバーのライフサイクル管理 (initialize / shutdown / register_channel)

           ┌──────────────────────┬──────────────────────────────┐
           │                      │                              │
      Null 実装              rdma_comm 実装
      (ENABLE_RDMA=OFF)      (ENABLE_RDMA=ON)
      ─────────────────────  ──────────────────────────────────────
      null_rdma_sender       rdma_comm_sender
                             └─ rdma::communication::rdma_sender をラップ

      null_rdma_send_stream  rdma_comm_send_stream
                             └─ rdma::communication::rdma_send_stream をラップ

      null_rdma_receiver     rdma_comm_receiver
                             └─ rdma::communication::rdma_receiver をラップ
```

---

## ディレクトリ構成

```
src/limestone/rdma/
  rdma_sender_base.h         — センダー抽象インターフェース
  rdma_send_stream_base.h    — 送信ストリーム抽象インターフェース
  rdma_receiver_base.h       — レシーバー抽象インターフェース
  rdma_receive_event.h       — 受信イベントのデータ型
  rdma_factory.h             — make_rdma_sender / make_rdma_receiver の宣言
  rdma_socket_io.h/.cpp      — 両実装で共用するソケット I/O ユーティリティ

  null_rdma_sender.h         — null センダーヘッダ
  null_rdma_send_stream.h    — null 送信ストリームヘッダ
  null_rdma_receiver.h       — null レシーバーヘッダ
  rdma_comm_sender.h         — rdma_comm センダーヘッダ（rdma-comm-lib に依存）
  rdma_comm_send_stream.h    — rdma_comm 送信ストリームヘッダ（rdma-comm-lib に依存）
  rdma_comm_receiver.h       — rdma_comm レシーバーヘッダ（rdma-comm-lib に依存）

  null/
    null_rdma_sender.cpp
    null_rdma_send_stream.cpp
    null_rdma_receiver.cpp
    rdma_factory_null.cpp    — null オブジェクトを返す make_rdma_sender / make_rdma_receiver

  rdma_comm/
    rdma_comm_sender.cpp
    rdma_comm_send_stream.cpp
    rdma_comm_receiver.cpp
    rdma_factory_rdma.cpp    — rdma_comm オブジェクトを返す make_rdma_sender / make_rdma_receiver
```

---

## ビルド時の切り替え（ファクトリ）

唯一の切り替えポイントは `rdma_factory.h` で宣言されている以下のファクトリ関数である。

```cpp
std::unique_ptr<rdma_sender_base>   make_rdma_sender(std::uint32_t slot_count);
std::unique_ptr<rdma_receiver_base> make_rdma_receiver(std::uint32_t slot_count);
```

| ビルドオプション   | コンパイルされるファクトリファイル          | 返されるオブジェクト                          |
|--------------------|---------------------------------------------|-----------------------------------------------|
| `ENABLE_RDMA=OFF`  | `null/rdma_factory_null.cpp`                | `null_rdma_sender`, `null_rdma_receiver`      |
| `ENABLE_RDMA=ON`   | `rdma_comm/rdma_factory_rdma.cpp`           | `rdma_comm_sender`, `rdma_comm_receiver`      |

どちらか一方のファクトリソースファイルのみがバイナリにリンクされる。limestone の残りのコードは常に `rdma_sender_base` / `rdma_receiver_base` だけを参照するため、`rdma-comm-lib` の型がメインコードベースに漏れ出ることはない。

---

## Null 実装の動作

| クラス                  | 動作                                                              |
|-------------------------|-------------------------------------------------------------------|
| `null_rdma_sender`      | `initialize()` / `get_send_stream()` は失敗を返す。`shutdown()` は成功する。 |
| `null_rdma_send_stream` | すべての送信操作はデータを転送せず即座に成功を返す。`send_with_writer()` はコールバックを呼び出さず成功を返す。 |
| `null_rdma_receiver`    | すべての操作は RDMA 処理を行わず即座に成功を返す。                |

---

## rdma-comm-lib との関係

`rdma-comm-lib` は以下の具体型を提供している。

- `rdma::communication::rdma_sender` — `rdma_comm_sender` がラップ
- `rdma::communication::rdma_send_stream` — `rdma_comm_send_stream` がラップ
- `rdma::communication::rdma_receiver` — `rdma_comm_receiver` がラップ

各ラッパーは `rdma-comm-lib` 固有の結果型を、基底クラスで定義された limestone 内部の結果構造体（`operation_result`、`send_result`、`flush_result`、`buffer_fill_result` など）に変換する。

---

## rdma_send_stream_base の API 一覧

| メソッド | 説明 |
|---|---|
| `send_bytes(payload, offset, length)` | ペイロードの一部を転送する（部分転送あり） |
| `send_all_bytes(payload, offset, length)` | 指定バイト数をすべて転送するまでリトライする |
| `send_with_writer(remaining_size, writer)` | ライブラリ内部バッファをコールバックで直接書き込んで送信する |
| `flush(timeout)` | 未完了のすべての ACK を受信するまで待機する |

### `send_with_writer` の概要

`send_with_writer` は rdma-comm-lib の `rdma_send_stream::send_with_writer` に対応する API で、コールバック（`buffer_writer`）を受け取り、ライブラリが確保した送信バッファに直接データを書き込んでから RDMA Write を発行する。ペイロードを `std::vector` で渡す `send_bytes` とは異なり、コピーなしで送信できる。

```cpp
using buffer_writer = std::function<buffer_fill_result(std::uint8_t* buffer, std::size_t capacity)>;

[[nodiscard]] virtual send_result send_with_writer(
    std::size_t   remaining_size,
    buffer_writer writer) noexcept = 0;
```

- `remaining_size` が 0 の場合はコールバックを呼ばず、`bytes_written == 0` の成功を返す。
- コールバックが失敗を返した場合はバッファを破棄し、RDMA Write は発行されない。
