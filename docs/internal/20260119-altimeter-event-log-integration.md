# 2026-01-19 Altimeterイベントログ組み込み作業メモ

## 作業ブランチ

feat/add-log-instance-id

## Altimeterの組み込み方法

`CMAKE_PREFIX_PATH` で有効化）。
- `altimeter` は `~/opt` にインストールし、`CMAKE_PREFIX_PATH=/home/umegane/opt` を使うことに決定。
- tsurugidb サブプロジェクトの例として、`altimeter` ターゲットのリンクと `ENABLE_ALTIMETER` ガードの使い方を確認。
- `.vscode/tasks.json` の configure タスクに `-DENABLE_ALTIMETER=ON` を追加。


altimeterのインストール -> altimeterのプロジェクトで以下を実行

```
cd logger/
mkdir -p build
cd build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=~/opt ..
cmake --build . --target install
```

## LimestoneがAltimeterで出力すべきログ

### 出力対象

https://github.com/project-tsurugi/altimeter/blob/feature/multi_nodes/docs/ja/altimeter_user_guide_basic.md#%E3%82%A4%E3%83%99%E3%83%B3%E3%83%88%E3%83%AD%E3%82%B0%E3%81%AE%E3%83%AD%E3%82%B0%E7%A8%AE%E9%A1%9E


上記URLのドキュメントのうち以下のログを出力する

- wal_stored
- wal_shipped
- wal_received
- wal_started


### 出力位置

- wal_stored
    - `datastore::update_min_epoch_id` 関数内の次の行の直後に追加する
            ```cpp
            TRACE_FINE << "epoch_id_record_finished_ updated to " << to_be_epoch;
            ```
- wal_shipped
    - `datastore::persist_and_propagate_epoch_id` 関数内の次の行の直後に追加する。2箇所存在する。
            ```cpp
            bool sent = impl_->propagate_group_commit(epoch_id);
            ```
- wal_received
    - `message_group_commit::post_receive` 関数内の次の行の直後に追加する
            ```cpp
                TRACE_START << "epoch_number: " << epoch_number_;
            ```
- wal_started
    - `datastore::ready() ` 関数内の次の行の直後に追加する
            ```cpp
            blob_id_type max_blob_id = std::max(create_snapshot_and_get_max_blob_id(), compaction_catalog_->get_max_blob_id());
            ```

## ログ出力項目と取得方法（wal_*）

Altimeterの仕様上、`wal_stored / wal_shipped / wal_received / wal_started` の各イベントは
共通で以下の項目が必要。

- `time`: Altimeter 側で自動付与（`log_item` が生成時刻を設定）
- `type`: `altimeter::event::type::wal_stored` など、イベント種別で固定
- `level`: `altimeter::event::level::log_data_store`（数値 30）
- `instance_id`: `configuration::instance_id_` 由来（datastore に引き渡す必要あり）
- `dbname`: **現状 limestone に保持なし**。上位層からの伝播 or 定数化を検討
- `pid`: `getpid()` で取得
- `wal_version`: **現状取得元なし**。WAL フォーマット識別子の定義が必要
- `result`: 成功/失敗（Altimeter 定義: success=1, failure=2）
- `master_instance_id`: `wal_received` のみ必須。**現状取得元なし**。レプリケーション設定から取得する設計が必要

### イベント別メモ

- `wal_stored`
  - 位置: `datastore::update_min_epoch_id`
  - `result`: `write_epoch_callback_` が例外なく完了したら成功

- `wal_shipped`
  - 位置: `datastore::persist_and_propagate_epoch_id`
  - `result`: `impl_->propagate_group_commit(epoch_id)` の戻り値で判断

- `wal_received`
  - 位置: `message_group_commit::post_receive`
  - `result`: 受信処理が例外なく完了したら成功

- `wal_started`
  - 位置: `datastore::ready`
  - `result`: 初期化処理が例外なく完了したら成功


### 出力項目


- `wal_stored`
- `wal_shipped`
- `wal_started`
    - time, type, instance_id, level, dbname, pid, wal_version, result
- `wal_received`
    - time, type, instance_id, level, dbname, pid, wal_version, master_instance_id, result

## 出力項目の取得方法

### 初期化時に保持してアクセッサで取得
- `instance_id`
- `pid`

### それ以外（方針/要検討）
- `time`: altimeter の `log_item` 生成時に自動付与
- `type`: イベントごとに固定値を設定(wal_stored, wal_shipped, wal_received, wal_started)
- `level`: `altimeter::event::level::log_data_store`
- `dbname`: 上位層からの伝播 or 定数化を検討
- `wal_version`: 取得元の定義が必要
- `master_instance_id`: レプリケーション設定から取得する設計が必要（`wal_received` のみ）
- `result`: 成否判定のルールをイベントごとに決定
