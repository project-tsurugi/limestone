# ファイルフォーマットの更新

BLOB対応に伴い、ファイルフォーマットのバージョンを更新する必要がある。
ファイルフォーマットのバージョンを記録している`limestone-manifest.json
に関聯している処理を洗い出し、必要におうじて修正する必要がある。

## ファイルのピックアップ

調査対象のファイルは以下の通り

**テストコード以外**
```
datastore.cpp
datastore_restore.cpp
internal.h
dblogutil.cpp
datastore_format.cpp
datastore.h
```
**tテストコード**
```
compaction_test_fixture.h
epoch_file_test.cpp
dblogutil_compaction_test.cpp
dblogutil_test.cpp
rotate_test.cpp
log_dir_test.cpp
testdata.cpp
log_channel_test.cpp
datastore_test.cpp
```

## テストコード以外の調査結果

* datastore.h
  * とくになし
* datastore.cpp
  * コンストラクタでいろいろやっている
    * マニフェストのパスの設定
    * ファイルリストへの追加
    * 2重起動防止のチェック=>マニフェストファイルをロックしている
    * datastore::begin_backup でマニフェストファイルをバックアップ対象に加えている
* datastore_restore.cpp
  * リストアするファイルのバージョンチェックを行っている。
* dblogutil.cpp
  * DB起動中にツールを使わないようにマニフェストファイルのロックを取得している。
* internal.h
  * マニフェストファイルのパスを定義している
  * is_supported_version() => サーポートバージョンのチェック
  * acquire_manifest_lock() ロックの取得

* datastore_format.cpp
  * setup_initial_logdir()
    * 初期ディレクトリの作成 => ここで、manifestファイルを作成する。
  * check_and_migrate_logdir_format
    * マニフェストファイルのマイグレーション(バージョン変更)を行う。
    * マイグレーション中に落ちたときのリカバリなども起こっている。


## 変更が必要な箇所のまとめ

* 