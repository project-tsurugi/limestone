# Limestonenの例外の取り扱い

## 現状の問題

[Discussions #964](https://github.com/project-tsurugi/tsurugi-issues/discussions/964)
[Issue #965](https://github.com/project-tsurugi/tsurugi-issues/issues/965)

Limnestoneの処理の中で、I/Oエラーが発生すると、std::runtime_errorがスローされるが、
この例外が適切に処理されないことがある。

## 対応方針

* 致命的なIOエラー発生時には例外をなげる
* 例外はLimestone独自の例外を定義する
* Limestoneは、外部に公開するAPIに対して、発生する可能性がある例外をドキュメント化する
* 呼び出し側がAPI仕様に合わせて、例外をハンドリグする。
* 呼び出し側の想定はShirakamiとブートシーケンス

## 現状調査

現行のAPIの中で例外をスローする可能性があるもの。
現在実装がないものは調査対象外とする。


* backup.h
    * なし
* backup_detail.h
    * なSI
* configuration.h
    * なし
* cursor.h
    * cursorクラスはnext()メソッド呼び出しにスナップショットをREADしメモリに展開するため、I/Oエラーが発生する可能性がある。他のメソッドはメモリに展開したデータを返すだけなので、I/Oエラーが発生する可能性はない。
    * 例外は、large_objects()メソッドで現在実装がないので断見できないが、large_objects()メソッドは、large_object_viewのリストを返すだけであり、実際のI/Oは、large_object_viewで行われるので、I/Oエラーが発生しないと考えられる。
    * cursor::next()
        * std::runtime_error
* datastore.h
    * datastore::datastore(configuration const& conf)
        * I/Oエラーが発生する可能性がある。
            * 指定のログディレクトリが存在せず、作成に失敗した場合
            * ログディレクトリの初期化に失敗した場合
    * status datastore::restore(std::string_view from, std::vector<file_set_entry>& entries)
        * I/Oエラーが発生する可能性がある。
            * I/Oエラー発生時に、ステータスでエラーを通知することがある。
                * ファイルが見つからない
                    * status::err_not_found;
                * ファイルがレギュラーファイルでない
                    * status::err_not_found;
                * コピーに失敗
                    * コピー失敗時に発生する例外のメッセージをログに出力するが、ステータスは、status::err_permission_errorを返す。
                * ファイルの削除に失敗
                    * ファイル削除失敗時に発生する例外のメッセージをログに出力するが、ステータスは、status::err_permission_errorを返す。
    * void datastore::ready()
        * I/Oエラーが発生する可能性がある。
            * ログディレクトリのファイル読み取りでエラーが発生した場合
            * std::runtime_errorが返る。
    * void switch_epoch(epoch_id_type epoch_id);
        * I/Oエラーが発生する可能性がある。
            * このメソッド呼び出し時に、ログチャネルのローテションが行われる可能性がある。
                * ログチャネルのローテーション時にI/Oエラーが発生すると、std::runtime_errorがスローされる。
                * このメソッド自体は、ローテーション時にstd::runtime_errorをスローせず、ローテーションをリクエストしたメソッドがスローすべき。
            * エポックファイルの更新に失敗すると、std::runtime_errorがスローされる。
    * std::unique_ptr<backup_detail> datastore::begin_backup(backup_type btype)
        * 現状I/Oエラーは発生しない    
        * ローテーション時にI/Oエラーが発生する可能性があるが、ローテーション処理は、switch_epoch()呼び出し時に行われるため。
        * 本来、このメソッドがI/Oエラーを返すべき。
    * tag_repository& datastore::epoch_tag_repository() noexcept
        * I/Oエラーは発生しない
    * void compact_with_online();
        * I/Oエラーが発生する可能性がある。
            * ログチャネルのローテーション時にI/Oエラーが発生すると、std::runtime_errorがスローされる。
            * コンパクション時にI/Oエラーが発生すると、std::runtime_errorがスローされる。
    * std::unique_ptr<snapshot> datastore::get_snapshot() const
        * I/Oエラーは発生しない
    * std::shared_ptr<snapshot> datastore::shared_snapshot() const
        * I/Oエラーは発生しない
    * log_channel& datastore::create_channel(const boost::filesystem::path& location)
        * I/Oエラーは発生しない
    * epoch_id_type datastore::last_epoch() const noexcept
        * I/Oエラーは発生しない
    * void datastore::recover([[maybe_unused]] const epoch_tag& tag)
        * 実装されていない
    * backup& datastore::begin_backup()    
        * I/Oエラーは発生しない
    * void datastore::add_persistent_callback(std::function<void(epoch_id_type)> callback) noexcept
        * I/Oエラーは発生しない
    * void datastore::switch_safe_snapshot([[maybe_unused]] write_version_type write_version, [[maybe_unused]] bool inclusive)
        * 未実装のため、I/Oエラーは発生しない
    * void datastore::add_snapshot_callback(std::function<void(write_version_type)> callback) noexcept
        * I/Oエラーは発生しない
    * std::future<void> datastore::shutdown() noexcept
        * I/Oエラーは発生しない



* epoch_id_type.h
    * なし
* epoch_tag.h
    * 使用されていないクラス
* file_set_entry.h
    * なし
* large_object_input.h
    * 使用されていないクラス
* large_object_view.h
    * 使用されていないクラス
* log_channel.h
* restore_progress.h
    * 使用されていないクラス
* snapshot.h
    * std::unique_ptr<cursor> snapshot::get_cursor() const
        * 現状例外はスローしないが、呼び出し時にI/Oエラーが発生するとstd::abort()が呼ばれる。
    * std::unique_ptr<cursor> snapshot::find([[maybe_unused]] storage_id_type storage_id, [[maybe_unused]] std::string_view entry_key) const noexcept
        * 現在未実装、実装後はI/Oエラーが発生する可能性があるが、noexceptと制限されている。
    * std::unique_ptr<cursor> snapshot::scan([[maybe_unused]] storage_id_type storage_id, [[maybe_unused]] std::string_view entry_key, [[maybe_unused]] bool inclusive) const noexcept
        * 現在未実装、実装後はI/Oエラーが発生する可能性があるが、noexceptと制限されている。
* storage_id_type.h
    * なし
* tag_repository.h
    * 使用されていないクラス
* write_version_type.h
    * なし


## 例外をスローしているコードから追跡

例外をスローしているソースファイル

src/limestone/compaction_catalog.cpp
src/limestone/datastore.cpp
src/limestone/datastore_format.cpp
src/limestone/datastore_snapshot.cpp
src/limestone/dblog_scan.cpp
src/limestone/dblogutil/dblogutil.cpp
src/limestone/log_channel.cpp
src/limestone/log_entry.h
src/limestone/online_compaction.cpp
src/limestone/online_compaction.h
src/limestone/parse_wal_file.cpp
src/limestone/sortdb_wrapper.h


## 例外をスローしている関数一覧

### compaction_catalog.cpp

* compaction_catalog::from_catalog_file => 済
* compaction_catalog::load_catalog_file => 済
* compaction_catalog::parse_catalog_entry => 済
* compaction_catalog::update_catalog_file => 済

### datastore.cpp

* datastore::datastore(configuration const& conf)  => 済
* datastore::ready  => 済
* datastore::update_min_epoch_id  => 済
* datastore::rotate_epoch_file  => 済

### datastore_format.cpp

* setup_initial_logdir  => 済
* check_and_migrate_logdir_format  => 済

### datastore_snapshot.cpp

* create_sorted_from_wals  => 済
* create_compact_pwal  => 済
* datastore::create_snapshot  => 済
* dblog_scan::scan_pwal_files  => 済
* scan_pwal_files_throws  => 済

### dblog_scan.cpp

* last_durable_epoch  => 済
* last_durable_epoch_in_dir  => 済
* log_error_and_throw => 済:未使用


### dblogutil.cpp

* make_tmp_dir_next_to => 済
* compaction => 済

### log_channel.cpp

* log_channel::begin_session => 済
* log_channel::end_session => 済
* log_channel::add_entry => 済


### log_entry.h

* read => 済
* write_uint8 => 追いきれない
* write_bytes => 追いきれない

### online_compaction.cpp

* select_files_for_compaction => 済
* ensure_directory_exists => 済
* handle_existing_compacted_file => 済
* get_files_in_directory => 済
* remove_file_safely => 済

### parse_wal_file.cpp

dblog_scan::scan_one_pwal_file => 済


### sortdb_wrapper.h

追わなくてよさそう

* put
* get
* each


## 例外をスローしている関数を呼んでいる関数

    * status datastore::restore(std::string_view from, std::vector<file_set_entry>& entries)
        * I/Oエラーが発生する可能性がある。
            * I/Oエラー発生時に、ステータスでエラーを通知することがある。
                * ファイルが見つからない
                    * status::err_not_found;
                * ファイルがレギュラーファイルでない
                    * status::err_not_found;
                * コピーに失敗
                    * コピー失敗時に発生する例外のメッセージをログに出力するが、ステータスは、status::err_permission_errorを返す。
                * ファイルの削除に失敗
                    * ファイル削除失敗時に発生する例外のメッセージをログに出力するが、ステータスは、status::err_permission_errorを返す。



* datastore::datastore()
    * check_and_migrate_logdir_format
* datastore::compact_with_online
    * create_compact_pwal
        * begin_session
            * write
        * create_sorted_from_wals
    * select_files_for_compaction
    * ensure_directory_exists
    * handle_existing_compacted_file
    * get_files_in_directory
    * remove_file_safely
* datastore::online_compaction_worker
    * ensure_directory_exists
* datastore::switch_epoch
    * update_min_epoch_id
        * durable_epoch
            * write_uint8

* datastore::ready
    * datastore::create_snapshot
        * create_sorted_from_wals
* datastore::rotate_epoch_file
  * rotation_taskから呼ばれる


* log_channel::begin_session
    * begin_session
        * write
* log_channel::end_session
    * end_session
        * write
    * update_min_epoch_id
        * durable_epoch
            * write_uint8
* log_channel::add_entry

* cursor::next
    * read

* dblogutil
    * compaction
        * create_compact_pwal
            * begin_session
                * write
            * create_sorted_from_wals
                * last_durable_epoch_in_dir
                    * last_durable_epoch
                        * read
        * last_durable_epoch_in_dir
            * last_durable_epoch
                * read
        * make_work_dir_next_to
            * make_tmp_dir_next_to
        * make_tmp_dir_next_to
            * make_tmp_dir_next_to
        * write_uint8
    * inspect
        * dblog_scan::scan_pwal_files
            * dblog_scan::scan_one_pwal_file
        * last_durable_epoch_in_dir
            * last_durable_epoch
                * read
    * repair
        * dblog_scan::scan_pwal_files
            * dblog_scan::scan_one_pwal_file
        * last_durable_epoch_in_dir
            * last_durable_epoch
                * read

## 特殊が対応な必要な個所


* datastore::online_compaction_worker
   * 独立したスレッドなので、例外を上位にスルーするのではなく、スレッド内で処理する必要がある。
     * 現状では、ログを出力してスレッドが終了してしまう。これはこのままとし、LOG-0.7対応時に対応する。
* std::unique_ptr<backup_detail> datastore::begin_backup(backup_type btype)   
    * I/Oエラーが発生する処理を別スレッドで行っているため、別スレッドの方でI/O例外が発生しているが、別スレッドでの例外を検知してこのスレッドの呼び出し元に例外を通志するべき
    * 対応済み
* std::abortを呼んでいる個所
    * 処理の統一のため、例外をスローするように変更する必要がある。
      * と思ったが、デストラクタでabortを呼んでいるケースがあり、単純に例外をスローするように変更するだけでは、意味のない対応になるため、とりあえず対応しないことにした。
* datastore::restore    
    * I/Oエラー発生時にstatusで戻り値を返す。
    * この関数(引数の違いで2つある)だけ、他とI/Fが違うが、このまま残してよいのか。
    * このまま残す。


