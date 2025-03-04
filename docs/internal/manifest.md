# ファイルフォーマットの更新

BLOB対応に伴い、ファイルフォーマットのバージョンを更新する必要がある。
ファイルフォーマットのバージョンを記録している`limestone-manifest.json
に関聯している処理を洗い出し、必要におうじて修正する必要がある。


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
* Version 2
  * オンラインコンパクション対応バージョン
  * コンパクション関聯ファイルが追加された。
* Version 3
  * BLOB対応バージョン
  * blobディレクトリ配下にBLOBファイルが保存されるようになった。
  * WALファイルに、BLOB付きのエントリが追加された。
  
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
* Version 2 対応のTsurugi
  * 起動時に、Version 1のデータをVersion 2に自動アップグレードする。
  * Version 3のデータを読むことはできない
    * 起動時にエラーとなる。
    * BLOBデータを含む場合、手動でもダウングレード不可
    * BLOBデータを含まない場合、手動ダウグレードは可能(未サポート)
      * `limestone-manifest.json` を手動で修正し、blobディレクトリを削除


## 永続化データ形式バージョンの変更

永続化データ形式バージョンの変更にともなう、ソースコードの修正

### Version 2 から Version 3 への更新

* datastore_restore.cpp
  * `check_manifest(const boost::filesystem::path& manifest_path)`
    * リスト時のバージョンチェックを行っている。
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

