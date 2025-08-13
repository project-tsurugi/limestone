# ファイルフォーマットの更新



### マニフェストファイルの形式

マニフェストファイルは永続化データの形式等の情報を記載したファイルで、 `${log_location}/limestone-manifest.json` に配置される。

マニフェストファイルは JSON 形式であり、以下のプロパティを有する。

プロパティ名 | 形式 | 設定値 | 概要
-------------|------|--------|-----
`format_version` | 十進数文字列 | `"1.0"` | マニフェストファイルの形式を表すバージョン (`major.minor`)
`persistent_format_version` | 整数 | `1` | 永続化データ形式バージョン

## 永続化データ形式バージョン

### バージョンについて

* Version 0
  * 最初期バージョン、`limestone-manifest.json` が存在しない場合、Version 0とみなす。

* Version 1
  * `limestone-manifest.json` 導入時のバージョン
  * GA-1はこのバージョン 
* Version 2
  * オンラインコンパクション対応バージョン
  * コンパクション関聯ファイルが追加された。
* Version 3
  * BLOB対応バージョン
  * blobディレクトリ配下にBLOBファイルが保存されるようになった。
  * WALファイルに、BLOB付きのエントリが追加された。
* Version 4
  * Limestoneのデータフォーマットは変更なし
  * jogasakiが管理するテーブル・インデックスのメタデータが拡張され、
    それにともなって永続化データが前方互換でなくなくなったため。Limestoneで管理しているバージョンを更新。
* Version 5
  * Manifestファイルの形式が変更された。
    * `format_version` が `"1.0"` から `"1.1"`
    * `persistent_format_version` が `4` から `5`に変更された。
    * `instance_uuid` が追加された。
      * ログディレクトリ作成時、またはフォーマットバージョン 4->5 のアップグレード時に生成したUUIDが設定される。
      * 主にレプリケーション時にマスタとレプリカが同一のデータベースであることの確認のために使用する。
* Version 6
  * WALファイルにmarker_endを書き込むように変更

* Version 7
  * wal_historyファイルを追加

### バージョン間の互換性

* Version 0 対応のTsurugi
  * 本ドキュメントの対象外
* Version 1 対応のTsurugi
  * 起動時に、Version 0のデータをVersion 1に自動アップグレードする。
  * Version 2のデータを読むことはできない
    * 起動時にエラーとなる。
    * 手動ダウグレードは可能(未サポート)
      * `limestone-manifest.json` を手動で修正し、コンパクション関聯のファイルを削除
  * Version 3のデータを読むことはできない
    * 起動時にエラーとなる。
    * BLOBデータを含む場合、手動でもダウングレード不可
    * BLOBデータを含まない場合、手動ダウグレードは可能(未サポート)
      * `limestone-manifest.json` を手動で修正し、コンパクション関聯のファイルと、blobディレクトリを削除
  * Version 4以降のデータを読むことはできない
    * 起動時にエラーとなる。
    * 手動でもダウングレード不可
* Version 2 対応のTsurugi
  * 起動時に、Version 1のデータをVersion 2に自動アップグレードする。
  * Version 3のデータを読むことはできない
    * 起動時にエラーとなる。
    * BLOBデータを含む場合、手動でもダウングレード不可
    * BLOBデータを含まない場合、手動ダウグレードは可能(未サポート)
      * `limestone-manifest.json` を手動で修正し、blobディレクトリを削除
  * Version 4以降のデータを読むことはできない
    * 起動時にエラーとなる。
    * 手動でもダウングレード不可
* Version 3 対応のTsurugi
  * 起動時に、Version 1,2のデータをVersion 3に自動アップグレードする。
  * Version 4以降のデータを読むことはできない
    * 起動時にエラーとなる。
    * 手動でもダウングレード不可
* Version 4 対応のTsurugi
  * 起動時に、Version 1〜3のデータをVersion 4に自動アップグレードする。
  * Version 5以降のデータを読むことはできない
    * 起動時にエラーとなる。
    * 手動でもダウングレード不可
* Version 5 対応のTsurugi
  * 起動時に、Version 1〜4のデータをVersion 5に自動アップグレードする。
  * Version 6以降のデータを読むことはできない
    * 起動時にエラーとなる。
    * 手動でもダウングレード不可
* Version 6 対応のTsurugi
  * 起動時に、Version 1〜5のデータをVersion 6に自動アップグレードする。
  * Version 7以降のデータを読むことはできない
    * 起動時にエラーとなる。
    * 手動でもダウングレードは可能
      * wal_historyを削除
      * manifest.jsonを修正

* Version7 対応のTsurgi
  * 起動時に、Version 1〜6のデータをVersion 7に自動アップグレードする。
  * wal_historyの処理
    * 起動時にlogdirが存在しない場合に、wal_historyファイルが生成され、epoch=0のエントリが書き込まれる。wal_historyは1エントリのみ保持する。
    * 起動時にlogdirが存在し、Version 1〜6の場合、マイグレーションごepoch=durable_epochのエントリが書き込まれる。wal_historyは1エントリのみ保持する。
    * 起動時にlogdirが存在し、Version 7の場合、epoch=durable_epochのエントリを追記する。
    * tglogutil でコンパクションしたときは、wal_historyの内容は変更されない。


## 永続化データ形式バージョンの変更

永続化データ形式バージョンの変更にともなう、ソースコードの修正

### Version 2 から Version 3 への更新

* datastore_restore.cpp
  * `check_manifest(const boost::filesystem::path& manifest_path)`
    * リストア時のバージョンチェックを行っている。
    * Version 1 以降であれば、起動時にVersion 3へ自動アップグレードされるので、Version 1以降であることをチェックすればOK
    * 現行コードからの変更なし
* datastore_format.cpp
  * `setup_initial_logdir(const boost::filesystem::path& logdir`
    * ここで `limestone-manifest.json` を作成している
    * Version 3 に変更する。
  * `check_and_migrate_logdir_format(const boost::filesystem::path& logdir)`
    * Version 1, 2のときにVersion 3にアップグレードするように変更する。
    * Version 1では、コンパクションカタログが存在しないので作成する
    * Version 2では、コンパクションカタログが存在するので、それを使用する。
      * 既存コードは、この処理がないので追加が必要。
  * `is_supported_version(const boost::filesystem::path& manifest_path, std::string& errmsg)`
    * サポートバージョンのチェック
    * Version 3もサポートバージョンに加える
* その他
  * テストコード内でのバージョンチェックを行っている箇所の修正

### Version 3 から Version 4 への更新

* datastore_restore.cpp
  * `check_manifest(const boost::filesystem::path& manifest_path)`
    * リストア時のバージョンチェックを行っている。
    * Version 1 以降であれば、起動時にVersion 4へ自動アップグレードされるので、Version 1以降であることをチェックすればOK
    * 現行コードからの変更なし
* datastore_format.cpp
  * `setup_initial_logdir(const boost::filesystem::path& logdir`
    * ここで `limestone-manifest.json` を作成している
    * Version 4 に変更する。
  * `check_and_migrate_logdir_format(const boost::filesystem::path& logdir)`
    * Version 1〜3のときにVersion 4にアップグレードするように変更する。
  * `is_supported_version(const boost::filesystem::path& manifest_path, std::string& errmsg)`
    * サポートバージョンのチェック
    * Version 4もサポートバージョンに加える
* その他
  * テストコード内でのバージョンチェックを行っている箇所の修正

### Version 4 から Version 5 への更新

* manifestに関する処理をmanifestクラスに集約
  * `manifest.cpp` と `manifest.h` を作成


### Version 5から Version 6 への更新

* default_persistent_format_versionを6に変更
* `manifest.cpp` の `check_and_migrate_logdir_format` 関数を修正
  * 戻り値をvoidから `migration_info` に変更
  * `migration_info` クラスを作成
  * `migration_info` クラスは、マイグレーションの情報を保持
  * version5以前からversion6(以降)へマイグレーションした場合、
    * `migration_info::requires_rotation()` がtrueを返すようにする。
    * これをみて、起動時にローテーションを行うことを想定している。

### Version 6から Version 7 への更新

* default_persistent_format_versionを7に変更

