# データストアモジュールのI/Fデザイン

2022-03-28 arakawa (NT)  
2022-07-20 horikawa (NT) ライフサイクル（改版）、進捗確認（追加）  
2022-08-17 horikawa (NT) limestone API revisement. (Issue #9)の内容を追加  

## この文書について

* 2022年度に開発予定の、トランザクションエンジンのログを格納するデータストアの I/F に関するデザイン
  * クラスと機能をつらつら記載するので、クラス図やコラボレーション図を適当に起こしてもらえると
  * const 性等は省略しているので、適宜判断のこと
* 開発コードは `limestone`

## 主な機能

* parallel load + group commit
  * -> epoch based CC のバックエンド
* Safe SS
  * off-line full scan (-> recovery)
  * on-line random access (index spill out)
    * -> `LOG-2`
* on-line backup
  * -> `BACKUP-1`
* point-in-time recovery
  * -> `PITR-1`
* blob store
  * -> `BLOB-1`
* statistics storage
  * -> TBD
* streaming replication
  * -> unplanned

## 機能デザイン

* パッケージは `limestore`

### ライフサイクル

* `class datastore`
  * `datastore::datastore(configuration conf)`
    * overview
      * 所定の設定でデータストアインスタンスを構築する
      * 構築後、データストアは準備状態になる
  * `datastore::~datastore()`
    * overview
      * データストアインスタンスを破棄する
    * note
      * この操作は、データストアが利用中であっても強制的に破棄する
  * `datastore::recover()`
    * overview
      * データストアのリカバリ操作を行う
    * note
      * この操作は `datastore::ready()` 実行前に行う必要がある
      * リカバリ操作が不要である場合、この操作は何もしない
    * throws
      * `recovery_error` リカバリが失敗した場合
    * limit
      * `LOG-0` - かなり時間がかかる場合がある
  * `datastore::ready()`
    * overview
      * データストアの準備状態を完了し、利用可能状態へ推移する
    * limit
      * `LOG-0` - logディレクトリに存在するWALファイル群からsnapshotを作成する処理を行うため、かなり時間がかかる場合がある
* `class configuration`
  * `configuration::data_locations`
    * overview
      * データファイルの格納位置 (のリスト)
    * note
      * このパスは WAL の出力先と相乗りできる
        * 配下に `data` ディレクトリを掘ってそこに格納する
  * `configuration::metadata_location`
    * overview
      * メタデータの格納位置
    * note
      * 未指定の場合、 `storage_locations` の最初の要素に相乗りする
      * SSDなどの低遅延ストレージを指定したほうがいい
* `class restore_result`
  * `restore_result::status`
    * overview
      * restore()の処理結果（ok, err_not_found, err_permission_error, err_broken_data, or err_unknown_error）
  * `restore_result::id`
    * overview
      * 各restore処理に付与される識別子
  * `restore_::path`
    * overview
      * エラーの原因となったファイル名
  * `restore_::detail`
    * overview
      * err_broken_dataの詳細を示す文字列

### スナップショット関連

* `class datastore`
  * `datastore::snapshot() -> snapshot`
    * overview
      * 利用可能な最新のスナップショットを返す
    * note
      * `datastore::ready()` 呼び出し以降に利用可能
      * スナップショットは常に safe SS の特性を有する
      * スナップショットはトランザクションエンジン全体で最新の safe SS とは限らない
    * note
      * thread safe
    * limit
      * `LOG-0` - `ready()` 以降変化しない
      * `LOG-1` - `ready()` 以降変化してもよいが、ユースケースが今のところない
  * `datastore::shared_snapshot() -> std::shared_ptr<snapshot>`
    * -> `snapshot()` の `std::shared_ptr` 版
* `class snapshot`
  * class
    * overview
      * データストア上のある時点の状態を表したスナップショット
    * note
      * thread safe
    * impl
      * スナップショットオブジェクトが有効である限り、当該スナップショットから参照可能なエントリはコンパクションによって除去されない
  * `snapshot::cursor() -> cursor`
    * overview
      * スナップショットの全体の内容を読みだすカーソルを返す
      * 返されるカーソルは `cursor::next()` を呼び出すことで先頭の要素を指すようになる
  * `snapshot::find(storage_id_type storage_id, std::string_view entry_key) -> cursor`
    * overview
      * スナップショット上の所定の位置のエントリに対するカーソルを返す
    * return
      * 返されるカーソルは `cursor::next()` を呼び出すことで対象の要素を指すようになる
      * そのようなエントリが存在しない場合、 `cursor::next()` は `false` を返す
    * since
      * `LOG-2`
  * `snapshot::scan(storage_id_type storage_id, std::string_view entry_key, bool inclusive) -> cursor`
    * overview
      * スナップショット上の所定の位置以降に存在する最初のエントリに対するカーソルを返す
    * return
      * 返されるカーソルは `cursor::next()` を呼び出すことで先頭の要素を指すようになる
      * そのようなエントリが存在しない場合、 `cursor::next()` は `false` を返す
    * since
      * `LOG-2`
* `class cursor`
  * class
    * overview
      * スナップショット上のエントリを走査する
    * note
      * thread unsafe
  * `cursor::next() -> bool`
    * overview
      * 現在のカーソルが次のエントリを指すように変更する
    * return
      * 次のエントリが存在する場合 `true`
      * そうでない場合 `false`
  * `cursor::storage() -> storage_id_type`
    * overview
      * 現在のカーソル位置にあるエントリの、ストレージIDを返す
  * `cursor::key(std::string buf)`
    * overview
      * 現在のカーソル位置にあるエントリの、キーのバイト列をバッファに格納する
  * `cursor::value(std::string buf)`
    * overview
      * 現在のカーソル位置にあるエントリの、値のバイト列をバッファに格納する
  * `cursor::large_objects() -> list of large_object_view`
    * overview
      * 現在のカーソル位置にあるエントリに関連付けられた large object の一覧を返す
    * since
      * `BLOB-1`
* `class large_object_view`
  * class
    * overview
      * large object の内容を取得するためのオブジェクト
    * note
      * thread safe
    * since
      * `BLOB-1`
  * `large_object::size() -> std::size_t`
    * overview
      * この large object のバイト数を返す
  * `large_object::open() -> std::istream`
    * overview
      * この large object の内容を先頭から読みだすストリームを返す
* MEMO
  * statistics info
    * snapshot から SST attached file/buffer を取り出せるようにする？
    * storage ID ごとに抽出

### データ投入

![load](datastore-if/load.drawio.svg)

* `class datastore`
  * `datastore::create_channel(path location) -> log_channel`
    * overview
      * ログの出力先チャンネルを追加する
    * param `location`
      * ログの出力先ディレクトリ
    * limit
      * この操作は `ready()` が呼び出される前に行う必要がある
  * `datastore::last_epoch() -> epoch_id_type`
    * overview
      * 永続化データ中に含まれる最大の epoch ID 以上の値を返す
    * note
      * この操作は、 `datastore::ready()` の実行前後のいずれでも利用可能 (`LOG-0` を除く)
    * impl
      * 再起動をまたいでも epoch ID を monotonic にするためにデザイン
    * limit
      * `LOG-0` - この操作は `ready()` が呼び出された後に行う必要がある
  * `datastore::switch_epoch(epoch_id_type epoch_id)`
    * overview
      * 現在の epoch ID を変更する
    * note
      * `datastore::ready()` 呼び出し以降に利用可能
      * epoch ID は前回の epoch ID よりも大きな値を指定しなければならない
  * `datastore::add_persistent_callback(std::function<void(epoch_id_type)> callback)`
    * overview
      * 永続化に成功した際のコールバックを登録する
    * note
      * この操作は、 `datastore::ready()` の実行前に行う必要がある
  * `datastore::switch_safe_snapshot(write_version_type write_version, bool inclusive)`
    * overview
      * 利用可能な safe snapshot の位置をデータストアに通知する
    * note
      * `datastore::ready()` 呼び出し以降に利用可能
      * write version は major, minor version からなり、 major は現在の epoch ID 以下であること
      * この操作の直後に当該 safe snapshot が利用可能になるとは限らない
      * `add_safe_snapshot_callback` 経由で実際の safe snapshot の位置を確認できる
      * `datastore::ready()` 直後は `last_epoch` を write major version とする最大の write version という扱いになっている
    * since
      * `LOG-2`
  * `datastore::add_snapshot_callback(std::function<void(write_version_type)> callback)`
    * overview
      * 内部で safe snapshot の位置が変更された際のコールバックを登録する
    * note
      * この操作は、 `datastore::ready()` の実行前に行う必要がある
    * note
      * ここで通知される safe snapshot の write version が、 `datastore::snapshot()` によって返される snapshot の write version に該当する
    * impl
      * index spilling 向けにデザイン
    * since
      * `LOG-2`
  * `datastore::shutdown() -> std::future<void>`
    * overview
      * 以降、新たな永続化セッションの開始を禁止する
    * impl
      * 停止準備状態への移行
* `class log_channel`
  * class
    * overview
      * ログを出力するチャンネル
    * note
      * thread unsafe
  * `log_channel::begin_session()`
    * overview
      * 現在の epoch に対する永続化セッションに、このチャンネルで参加する
    * note
      * 現在の epoch とは、 `datastore::switch_epoch()` によって最後に指定された epoch のこと
  * `log_channel::end_session()`
    * overview
      * このチャンネルが参加している現在の永続化セッションについて、このチャンネル内の操作の完了を通知する
    * note
      * 現在の永続化セッションに参加した全てのチャンネルが `end_session()` を呼び出し、かつ現在の epoch が当該セッションの epoch より大きい場合、永続化セッションそのものが完了する
  * `log_channel::abort_session(error_code_type error_code, std::string message)`
    * overview
      * このチャンネルが参加している現在の永続化セッションをエラー終了させる
  * `log_channel::add_entry(...)`
    * overview
      * 現在の永続化セッションにエントリを追加する
    * param `storage_id : storage_id_type`
      * 追加するエントリのストレージID
    * param `key : std::string_view`
      * 追加するエントリのキーバイト列
    * param `value : std::string_view`
      * 追加するエントリの値バイト列
    * param `write_version : write_version_type` (optional)
      * 追加するエントリの write version
      * 省略した場合はデフォルト値を利用する
    * param `large_objects : list of large_object_input` (optional)
      * 追加するエントリに付随する large object の一覧
      * since `BLOB-1`
  * `log_channel::remove_entry(storage_id, key, write_version)`
    * overview
      * エントリ削除を示すエントリを追加する。
    * param `storage_id : storage_id_type`
      * 削除対象エントリのストレージID
    * param `key : std::string_view`
      * 削除対象エントリのキーバイト列
    * param `write_version : write_version_type`
      * 削除対象エントリの write version
    * note
      * 現在の永続化セッションに追加されている当該エントリを削除する操作は行わない。
      * 現在の永続化セッションで保存されたlogからのrecover()操作において、削除対象エントリは存在しないものとして扱う。
  * `log_channel::add_storage(storage_id, write_version)`
    * overview
      * 指定のストレージを追加する
    * param `storage_id : storage_id_type`
      * 追加するストレージのID
    * param `write_version : write_version_type`
      * 追加するストレージの write version
    * impl
      * 無視することもある
  * `log_channel::remove_storage(storage_id, write_version)` 
    * overview
      * 指定のストレージ、およびそのストレージに関するすべてのエントリの削除を示すエントリを追加する。
    * param `storage_id : storage_id_type`
      * 削除対象ストレージのID
    * param `write_version : write_version_type`
      * 削除対象ストレージの write version
    * note
      * 現在の永続化セッションに追加されている削除対象エントリを削除する操作は行わない。
      * 現在の永続化セッションで保存されたlogからのrecover()操作において、削除対象エントリは存在しないものとして扱う。
  * `log_channel::truncate_storage(storage_id, write_version)` 
    * overview
      * 指定のストレージに含まれるすべてのエントリ削除を示すエントリを追加する。
    * param `storage_id : storage_id_type`
      * 削除対象ストレージのID
    * param `write_version : write_version_type`
      * 削除対象ストレージの write version
    * note
      * 現在の永続化セッションに追加されている削除対象エントリを削除する操作は行わない。
      * 現在の永続化セッションで保存されたlogからのrecover()操作において、削除対象エントリは存在しないものとして扱う。
* `class large_object_input`
  * `class`
    * overview
      * large object を datastore に追加するためのオブジェクト
    * impl
      * requires move constructible/assignable
    * since
      * `BLOB-1`
  * `large_object_input::large_object_input(std::string buffer)`
    * overview
      * ファイルと関連付けられていない large object を作成する
  * `large_object_input::large_object_input(path_type path)`
    * overview
      * 指定のファイルに内容が格納された large object を作成する
    * note
      * 指定のファイルは移動可能でなければならない
  * `large_object_input::locate(path_type path)`
    * overview
      * この large object の内容を指定のパスに配置する
      * このオブジェクトが `detach()` を呼び出し済みであった場合、この操作は失敗する
      * この操作が成功した場合、 `detach()` が自動的に呼び出される
  * `large_object_input::detach()`
    * overview
      * この large object の内容を破棄する
      * このオブジェクトがファイルと関連付けられていた場合、この操作によって当該ファイルは除去される
  * `large_object_input::~large_object_input()`
    * overview
      * このオブジェクト破棄する
      * このオブジェクトとがファイルと関連付けられていた場合、そのファイルも除去される

### バックアップ

* `class datastore`
  * `datastore::begin_backup() -> backup`
    * overview
      * バックアップ操作を開始する
    * note
      * この操作は `datastore::read()` 呼び出しの前後いずれでも利用可能
    * since
      * `BACKUP-1`
  * `datastore::restore(std::string_view from, bool keep_backup) -> restore_result`
    * overview
      * データストアのリストア操作を行う
      * keep_backupがfalseの場合は、fromディレクトリにあるWALファイル群を消去する
    * note
      * この操作は `datastore::ready()` 実行前に行う必要がある
      * `LOG-0`のリストア操作は、fromディレクトリにバックアップされているWALファイル群をlogディレクトリにコピーする操作となる
* `class backup`
  * class
    * overview
      * バックアップ操作をカプセル化したクラス
      * 初期状態ではバックアップ待機状態で、 `backup::wait_for_ready()` で利用可能状態まで待機できる
    * note
      * バックアップは、その時点で pre-commit が成功したトランザクションが、durable になるのを待機してから、それを含むログ等を必要に応じて rotate 等したうえで、バックアップの対象に含めることになる
      * durable でないコミットが存在しない場合、即座に利用可能状態になりうる
    * since
      * `BACKUP-1`
  * `backup::is_ready() -> bool`
    * overview
      * 現在のバックアップ操作が利用可能かどうかを返す
  * `backup::wait_for_ready(std::size_t duration) -> bool`
    * overview
      * バックアップ操作が利用可能になるまで待機する
  * `backup::files() -> list of path`
    * overview
      * バックアップ対象のファイル一覧を返す
    * note
      * この操作は、バックアップが利用可能状態でなければならない
  * `backup::~backup()`
    * overview
      * このバックアップを終了する
    * impl
      * バックアップ対象のファイルはGCの対象から外れるため、バックアップ終了時にGC対象に戻す必要がある

### 世代管理

* `class datastore`
  * `datastore::epoch_tag_repository() -> tag_repository`
    * overview
      * epoch tag のリポジトリを返す
    * note
      * `datastore::ready()` 呼び出しの前後いずれでも利用可能
    * since
      * `PITR-1`
  * `datastore::recover(epoch_tag)`
    * overview
      * データストアの状態を指定されたエポックの時点に巻き戻す
    * note
      * この操作は `datastore::ready()` 実行前に行う必要がある
      * この操作によって、指定された epoch 以降のデータはすべて失われる
      * この操作によって、指定された epoch 以降を指す epoch タグは無効化される
    * throws
      * `recovery_error` リカバリが失敗した場合
    * since
      * `PITR-1`
* `class tag_repository`
  * `tag_repository::list() -> list of epoch_tag`
    * overview
      * 登録された epoch タグの一覧を返す
    * since
      * `PITR-1`
  * `tag_repository::register(std::string name, std::string comments) -> std::future<epoch_tag>`
    * overview
      * 現在の epoch を epoch タグとして登録する
    * note
      * 同名の epoch タグを複数登録できない
    * note
      * ここまでに pre-commit されたデータを保護するため、 `backup` と同様にそれらが durable になるまでに多少時間を要する
  * `tag_repository::find(std::string_view name) -> std::optional<epoch_tag>`
    * overview
      * 指定の名前を持つ epoch タグを返す
    * note
      * そのようなタグが存在しない場合、 `std::nullopt` が返る
  * `tag_repository::unregister(std::string_view name)`
    * overview
      * 指定の名前を持つ epoch タグを削除する
    * note
      * そのようなタグが存在しない場合、特に何も行わない
* `class epoch_tag`
  * class
    * overview
      * 特定のエポックに関連付けられたタグ
      * タグが存在する限り、その時点のデータストアの状態に巻き戻せることが保証される
    * note
      * thread safe
    * since
      * `PITR-1`
  * `epoch_tag::name() -> std::string_view`
    * overview
      * タグ名を返す
  * `epoch_tag::comments() -> std::string_view`
    * overview
      * コメントを返す
  * `epoch_tag::epoch_id() -> epoch_id_type`
    * overview
      * 対応する epoch ID を返す
  * `epoch_tag::timestamp() -> std::chrono::system_clock::time_point`
    * overview
      * タグが作成された時刻を返す

### 進捗確認

* `class datastore`
  * `datastore::restore_status() -> restore_progress`
    * overview
      * 現在進行している、もしくは、直前に終了したrestore処理（以下、当該restore）の状態を返す
    * note
      * limestone起動後にrestore処理が1回も行われていない場合、statusはerr_not_foundとなる
* `class restore_progress`
  * `restore_progress::status`
    * overview
      * restore_status()による問い合わせ処理の結果（ok, err_not_found, or err_unknow_err）
    * note
      * 以下のフィールド（status_kind, source, progress）にはstatusがokの場合にのみ有効な値が入る。それ以外の場合は不定。
  * `restore_progress::status_kind`
    * overview
      * 当該restoreの処理状態または処理結果（preparing, running, completed, failed, or canceled）
  * `restore_progress::source`
    * overview
      * 当該restoreのsourceを示す文字列
  * `restore_progress::progress`
    * overview
      * 当該restoresの進捗率 (0.0～1.0のfloat値)