# BLOB API

本ドキュメントは、BLOB対応のためにLimestoneに追加および変更されるAPIについて記述する。

このドキュメントは、BLOB対応に伴う他モジュールとの連携および仕様調整を目的として作成する。
記載内容は作成時点の仕様に基づいているが、将来の仕様変更に際して本ドキュメントが必ずしも
更新されるわけではない。そのため、最新の仕様については、必ずソースコードを参照すること。

本ドキュメントでは、追加および変更されるヘッダごとに、対応するAPIについて記述する。
APIの詳細については、各ヘッダのコメントを参照すること。

## blob_pool.h の追加

**新規追加項目**

* blob_id_type
    * blob参照を表す型
  
* class blob_pool
    * BLOB プールの作成、破棄、および BLOB データの仮登録のためのクラス。
    * BLOB プールは、`datastore::acquire_blob_pool()` で取得可能。

## blob_file.h の追加

**新規追加項目**

* class blob_file
    * BLOB データにアクセスするためのクラス。
    * BLOB ファイルのインスタンスは、`datastore::get_blob_file(blob_id_type reference)` で取得可能。
    * BLOB ファイルから BLOB を保存しているファイルのパスを取得し、ファイルを読むことによりBLOBデータにアクセスできる。


## datastore.h の修正

**追加メソッド**

* `std::unique_ptr<blob_pool> datastore::acquire_blob_pool()`
  * BLOB プールの取得のためのメソッド。

* `blob_file datastore::get_blob_file(blob_id_type reference)`
  * BLOB ファイルの取得のためのメソッド。

* `void switch_available_boundary_version(write_version_type version)`
  * LimestoneがBLOBデータを削除するには、その BLOB への参照を有するバージョンのエントリが誰からも参照されなくなっていることが必要となる。
  これを判断するための情報は、 CC からデータストアに通知するためのメソッド。
  * LimestoneはGCによるBLOBデータ削除時に、削除可能なデータの判断のためにこのメソッドで通知された値を使用する。
    * コンパクション時のGCで、このメソッドで通知された値を使用する。
    * 起動時のGCでは、永続化データから参照されていないBLOBデータを無条件で削除する。

## log_channel.h の修正

**add_entry メソッドのシグネチャ変更**

**修正前**
```cpp
void add_entry(
    storage_id_type storage_id,
    std::string_view key,
    std::string_view value,
    write_version_type write_version,
    const std::vector<large_object_input>& large_objects
);
```

**修正後**
```cpp
void add_entry(
    storage_id_type storage_id,
    std::string_view key,
    std::string_view value,
    write_version_type write_version,
    const std::vector<blob_id_type>& large_objects
);
```


## large_object_input.h, large_object_view.h の削除

BLOB対応の方針変更により、以下のクラスを廃止しました。それに伴い、該当ヘッダファイルを削除しています。

* large_object_input クラス
* large_object_view クラス


## cursor.h の修正

BLOB対応の方針変更により、以下のメソッドを廃止しました。

*  `std::vector<large_object_view>& large_objects();`







