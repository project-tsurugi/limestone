# RDMA 抽象化レイヤー

## 概要

`src/limestone/rdma/` ディレクトリは Bridge パターンを用いて実装されており、`rdma-comm-lib` への依存を `limestone` コードベースの残りの部分から完全に隔離している。

limestone の内部コードは、このレイヤーで定義された抽象インターフェースとのみやり取りする。具体的な実装（`rdma-comm-lib` を使うものと、何もしない null 実装）はビルド時にファクトリ関数を通じて選択される。

---

## クラス階層

```
抽象インターフェース（limestone 内部、rdma-comm-lib への依存なし）
  rdma_sender_base          — センダーのライフサイクル管理
                              (initialize / get_send_stream / finalize_channel_setup / shutdown)
  rdma_send_stream_base     — チャネルごとのデータ送信 (acquire_frame_buffer /
                              submit_frame_buffer / flush。send_all_bytes は基底の非仮想ヘルパ)
  rdma_frame_buffer_base    — 送信リングから貸し出される書き込み領域 (payload / capacity)
  rdma_receiver_base        — レシーバーのライフサイクル管理 (initialize / get_dma_address /
                              finalize_channel_setup_with_sender / shutdown)
  handshake_connector_base  — handshake 接続側 (start / receive_response / send_finalize /
                              send_ready / receive_completion)
  handshake_acceptor_base   — handshake 受理側 (wait_for_start / send_response /
                              receive_finalize / complete)

      Null 実装 (ENABLE_RDMA=OFF)    rdma_comm 実装 (ENABLE_RDMA=ON)
      ─────────────────────────────  ──────────────────────────────────────────────
      null_rdma_sender               rdma_comm_sender
                                     └─ rdma::communication::rdma_sender をラップ
      null_rdma_send_stream          rdma_comm_send_stream
      null_rdma_frame_buffer         rdma_comm_frame_buffer
                                     └─ rdma::communication::rdma_send_stream をラップ
      null_rdma_receiver             rdma_comm_receiver
                                     └─ rdma::communication::rdma_receiver をラップ
      null_handshake_connector       rdma_comm_handshake_connector
                                     └─ rdma::handshake::handshake_connector をラップ
      null_handshake_acceptor        rdma_comm_handshake_acceptor
                                     └─ rdma::handshake::handshake_acceptor をラップ
```

---

## ディレクトリ構成

```
src/limestone/rdma/
  rdma_sender_base.h              — センダー抽象インターフェース
  rdma_send_stream_base.h/.cpp    — 送信ストリーム抽象インターフェース
                                    (send_all_bytes の共通実装を含む)
  rdma_frame_buffer_base.h        — 送信フレームバッファ抽象インターフェース
  rdma_receiver_base.h            — レシーバー抽象インターフェース
  rdma_receive_event.h            — 受信イベントのデータ型
  handshake_client_base.h         — handshake 接続側 / 受理側の抽象インターフェース
  rdma_factory.h                  — ファクトリ関数の宣言 (下記)
  rdma_handshake_payload.h/.cpp   — handshake の start / response ペイロードのコーデック
  rdma_replication_message_io.h/.cpp — RDMA 送信経路用の replication_message_io
                                    (send_blob を RDMA フレームへの直接書き込みに差し替え)

  null_*.h / rdma_comm_*.h        — 各実装のヘッダ (rdma_comm_*.h は rdma-comm-lib に依存。
                                    null_rdma_frame_buffer は null_rdma_send_stream.h に同居)

  null/
    null_rdma_sender.cpp
    null_rdma_send_stream.cpp
    null_rdma_receiver.cpp
    null_handshake_connector.cpp
    null_handshake_acceptor.cpp
    rdma_factory_null.cpp         — null オブジェクトを返すファクトリ

  rdma_comm/
    rdma_comm_sender.cpp
    rdma_comm_send_stream.cpp
    rdma_comm_receiver.cpp
    rdma_comm_handshake_connector.cpp
    rdma_comm_handshake_acceptor.cpp
    rdma_comm_handshake_result_conversion.h/.cpp
    rdma_comm_constants.h         — スロットサイズ等の定数 (rdma-comm-lib と static_assert で同期)
    rdma_factory_rdma.cpp         — rdma_comm オブジェクトを返すファクトリ
```

---

## ビルド時の切り替え（ファクトリ）

null / rdma_comm 実装のリンク切り替えは、`rdma_factory.h` で宣言されている以下のファクトリ
関数に集約されている (このほかビルド分岐としては、ENABLE_RDMA=OFF ビルドで RDMA モードの
設定自体を拒否する検証が `replication_config_loader.cpp` にある)。
センダー / レシーバーはデータ経路と ACK 経路で用途別に分かれている。

```cpp
std::unique_ptr<rdma_sender_base>   make_rdma_data_sender(std::uint32_t slot_count);   // master: データ送信
std::unique_ptr<rdma_sender_base>   make_rdma_ack_sender(std::uint32_t slot_count);    // replica: ACK 送信
std::unique_ptr<rdma_receiver_base> make_rdma_data_receiver(std::uint32_t slot_count); // replica: データ受信
std::unique_ptr<rdma_receiver_base> make_rdma_ack_receiver(std::uint32_t slot_count);  // master: ACK 受信

handshake_connector_create_result make_handshake_connector(
    std::string const& daemon_socket_path, std::chrono::milliseconds operation_timeout);
handshake_acceptor_create_result make_handshake_acceptor(
    std::string const& daemon_socket_path, std::chrono::milliseconds operation_timeout,
    std::optional<std::chrono::milliseconds> start_wait_timeout = std::nullopt);
```

| ビルドオプション   | コンパイルされるファクトリファイル | 返されるオブジェクト |
|--------------------|-------------------------------------|----------------------|
| `ENABLE_RDMA=OFF`  | `null/rdma_factory_null.cpp`        | `null_*`             |
| `ENABLE_RDMA=ON`   | `rdma_comm/rdma_factory_rdma.cpp`   | `rdma_comm_*`        |

どちらか一方のファクトリソースファイルのみがバイナリにリンクされる。limestone の残りのコードは常に抽象インターフェースだけを参照するため、`rdma-comm-lib` の型がメインコードベースに漏れ出ることはない。

---

## 送信モデル (acquire / submit)

送信は「送信バッファを借りて直接書く」モデルである。

1. `acquire_frame_buffer(max_payload, min_capacity)` で送信リングの一部
   (`rdma_frame_buffer_base`) を借りる。
2. 呼び出し側が `payload()` の領域へ直接書き込む。貸与される `capacity()` は要求量と
   異なりうるため、書いてよいのは「実際に持っているバイト数と `capacity()` の小さい方」まで。
3. `submit_frame_buffer(frame, payload_size)` がフレームヘッダを書き、RDMA Write を発行する。

フレームは受信側の ACK までリングスロットを占有するため、詰まった相手はやがて
`acquire_frame_buffer()` をブロックさせる (これがフロー制御)。上記のクランプを含む
送信ループの定型は基底の非仮想ヘルパ `send_all_bytes(payload)` が実装しており、
呼び出し側は通常こちらを使う。`flush(timeout)` は未 ACK のフレームが無くなるまで待つ。

---

## Null 実装の動作

| クラス | 動作 |
|--------|------|
| `null_rdma_sender` | `initialize()` / `get_send_stream()` / `finalize_channel_setup()` は失敗を返す。`shutdown()` は成功する。 |
| `null_rdma_send_stream` | `acquire_frame_buffer()` はヒープバッファ (`null_rdma_frame_buffer`) を貸し出す (引数が契約外なら `nullptr`)。`submit_frame_buffer()` は書き込まれた内容を破棄して成功を返す (`payload_size` が capacity 超過なら失敗)。`flush()` は成功する。 |
| `null_rdma_receiver` | `initialize()` / `finalize_channel_setup_with_sender()` は失敗を返し、`get_dma_address()` は `std::nullopt` を返す。`shutdown()` は成功する。 |
| `null_handshake_connector` / `null_handshake_acceptor` | すべての操作が失敗を返す。 |

「RDMA 無効による失敗」の理由文字列はいずれも "RDMA is not enabled in this build
(ENABLE_RDMA=OFF)" である (`null_rdma_send_stream` の引数検査による失敗は別文言)。

---

## rdma-comm-lib との関係

`rdma-comm-lib` の以下の具体型をラップしている。

- `rdma::communication::rdma_sender` — `rdma_comm_sender` がラップ
- `rdma::communication::rdma_send_stream` — `rdma_comm_send_stream` がラップ
- `rdma::communication::rdma_receiver` — `rdma_comm_receiver` がラップ
- `rdma::handshake::handshake_connector` — `rdma_comm_handshake_connector` がラップ
- `rdma::handshake::handshake_acceptor` — `rdma_comm_handshake_acceptor` がラップ

handshake クライアントは、ローカルの handshake daemon (`rdma_handshaked`) の UNIX ドメイン
ソケットに接続し、DMA アドレス交換を daemon 経由で行う (TCP は使わない)。

各ラッパーは `rdma-comm-lib` 固有の結果型を、基底クラスで定義された limestone 内部の結果構造体
(`operation_result`、`send_result`、`flush_result`、`stream_acquire_result` など) に変換する
(handshake 系は `rdma_comm_handshake_result_conversion` に集約)。
