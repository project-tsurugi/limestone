 # ローテーションとコンパクション

本ドキュメントでは、LOG-0.6でのローテーション処理とコンパクション処理
について記述する。

## ローテーション

LOG-0.5以前のローテーションには以下の問題がある。

1. ローテーション終了後も、ローテーション済みのファイルへの書き込みが行われる可能性があり、一部データが欠損したローテーション済みファイルを使用した処理が行われることがある。
2. 複数のローテーションリクエストを同時に正しく処理することはできない。
3. ローテーション中にエポックの切り替えが発生し、特定のエポックのデータの一部がローテーション済みファイル、残りがローテーション後のアクティブなPWALファイルに書き込まれる可能性がある。
4. 3.と同じ原因で、ローテーション済みのPWALファイルと、ローテーション済みのエポックファイルのデュラブルエポックIDが一致しない可能性がある。
5. ローテーション完了後、エポックの切り替えの開始前に開始したログチャネルのセッションのデータはローテーション済みのPWALファイルに書かれるべきだが、書かれない。
6. ローテーション済みのファイルのリストの作成などをローテーション後に行っているため、連続してローテーションが行われたときに、不正なリストが作成されてしまう。

これらの問題はローテーションの実行タイミングの問題である。LOG-0.6では、これらの問題を解決するために以下の制御を
追加する。

1. ローテーションの要求に対して即座に処理を開始せずにリクエストをキューに入れ、リクエストの完了を待つ。
2. エポックの切り替え処理に先立ち、ローテーション要求のキューを確認しキューにリクエストがある場合は、ローテーション処理を行う。具体的にはPWALとエポックファイルのリネームを行う。1回のエポック切り替えで、高々1回しかリクエストを処理しない。
3. PWALとエポックファイルのリネームが終了したらエポックの切り替え処理を継続する。
4. 2.の処理時にアクティブなセションを持つログチャネルが存在する場合、アクティブなセッションがすべて終了するのを待ち、ローテーション要求に応答する。この処理は、3の処理と非同期に行う。
5. ローテーション後のファイルのリストをローテーションの要求もとに戻す。
   * コンパクション処理のために、このローテーション処理でローテーションされたPWALファイルだけでなく、全てのローテーション済みファイルのリストが必要である。
   * その他の必要な情報は、現行のコードに準じる。


## コンパクション

* 同時に複数のコンパクション処理を行わないように制御する。
* コンパクションが要求されたら、まずローテーションを行う。
* ローテーションが終了したら、コンパクションの対象となるPWALファイルを特定する。
  * 未ローテートのPWALファイルはコンパクションに対象ではない。
  * ローテーション済みのPWALファイルのリストから、コンパクションの対象となるファイルを選択する。
  * コンパクション処理中にローテーションが行われて、ローテーション済みのPWALファイルが増えても、これらのファイルはコンパクションの対象とはしない。このため、ローテーション済みファイルのリストは、ファイルシステムを調べて作成するのではなく、ローテーション処理の結果から取得する必要がある。
  * コンパクションカタログを調べ、コンパクション済みと記録されているPWALファイルはコンパクションの対象から外す。
  * 残ったローテーション済みのPWALファイルについては、当該ファイルに記録されている最初のセッションのエポックIDを取り出し、コンパクションカタログのエポックIDより大きなエポックIDであったPWALファイルのみをコンパクションの対象とする。
  * LOG-0.5のオフラインコンパクションツールと同じロジックでコンパクションを行う。
    * コンパクションの対象となるファイルは、コンパクションの対象となったPWALファイルと、前回のコンパクション処理で作成したコンパクション済みファイルである。
    * コンパクション済みファイルのファイル名は、LOG-0.6の使用に合わせて決定する。
    * オフラインコンパクションツールと異なり、コンパクションの対象となったPWALファイルは削除しない
  * コンパクションカタログについては、[コンパクション済みファイルとコンパクションカタログ](file_format.md)にしたがって処理する。


## 起動時のスナップショット作成

* LOG-0.5とLOG-0.6で、スナップショット作成対象となるファイルが異なる。
  * LOG-0.5では全PWALファイルが対象
  * LOG-0.6では未ローテートのPWALファイルとコンパクション済みファイル、および未コンパクションのローテート済みPWALファイルが対象となる。
  * 未ローテートのPWALファイルはコンパクションカタログを用いて、コンパクション対象のローテート済みPWALファイル作成時と同様の方法で選択する。ただし、ローテート済みファイルのリストはローテート処理の結果ではなく、ファイルシステムから取得する。


## コンパクションのトリガ

  とりあえず簡単に動くものをつくる

  * コンパクション用のスレッドを1つよういする。datastoreの初期化時にスレッドを生成し、シャットダウン時にスレッドを停止する。
  * このスレッドは、ログディレクトリの"start_compaction"というファイル名のファイルの有無を確認する。
  * ファイルの存在を確認したら、ファイルを削除し、コンパクション処理を実行し、実行結果を"comaction_result"というファイルに出力する。
  * このしくみにより、自動的にコンパクションが同時に実行されなくなる。
  * "start_compaction"ファイルを作成し、"comaction_result"の内容を表示するコマンドを作成する

## コンパクションスレッド

* 初期化終了時にコンパクションスレッドを作成する
* datasotreのシャットダウン時と、datastoreのデストラクタ呼び出し時ににスレッドを停止する。
* コンパクションスレッドは、ログディレクトリに`ctrl`ディレクトリが存在しない場合、作成する。
* `ctrl`ディレクトリに`start_compaction`というファイル名のファイルが作成されたら、コンパクションを開始し、コンパクションが終了するとstawrt_compaction処理を終了する。


## 現行のソート処理について

## 呼び出し階層

* スナップショット作成

datastore::ready　-> datastore::create_snapshot -> create_sortdb_from_wals(datastore_snapshot.cpp)


* WALのコンパクション
compaction(dblogutil.cpp) -> create_comapct_pwal(datastore_snapshot.cpp) -> create_sortdb_from_wals(datastore_snapshot.cpp)

datastore_snapshot.cpp の　create_sortdb_from_wals　がソート処理の本体

## create_sortdb_from_wals

sortdbの作成


```
#if defined SORT_METHOD_PUT_ONLY
    auto sortdb = std::make_unique<sortdb_wrapper>(from_dir, comp_twisted_key);
#else
    auto sortdb = std::make_unique<sortdb_wrapper>(from_dir);
#endif
```

* sortdb_warpperのインスタンス作成 -> 作業ディレクトリのクリアなどを行っている。
* ディレクトリのパスを渡しているが、コンストラクタでは内部へんすうに格納するのみ
* オンラインコンパクション用に、コンパクション対象ファイルのファイル名を渡す仕組みが必要

```
    dblog_scan logscan{from_dir};
```
* logscanインスタンスの作成
* コンストラクタは次のように定義されていて、変数の初期化しか行っていない。
  ```
    explicit dblog_scan(const boost::filesystem::path& logdir) : dblogdir_(logdir) { }
    
  ```

```
    epoch_id_type ld_epoch = logscan.last_durable_epoch_in_dir();
```

* エポックファイルからエポックIDを抽出

```
#if defined SORT_METHOD_PUT_ONLY
    auto add_entry = [&sortdb](log_entry& e){insert_twisted_entry(sortdb.get(), e);};
    bool works_with_multi_thread = true;
#else
    auto add_entry = [&sortdb](log_entry& e){insert_entry_or_update_to_max(sortdb.get(), e);};
    bool works_with_multi_thread = false;
#endif
```
* ログエントリをソートDBに挿入するためのラムダ式を定義
* insert_twisted_entry, insert_entry_or_update_to_maxは次のように定義されている。

```
static void insert_twisted_entry(sortdb_wrapper* sortdb, log_entry& e) {
    // key_sid: storage_id[8] key[*], value_etc: epoch[8]LE minor_version[8]LE value[*], type: type[1]
    // db_key: epoch[8]BE minor_version[8]BE storage_id[8] key[*], db_value: type[1] value[*]
    std::string db_key(write_version_size + e.key_sid().size(), '\0');
    store_bswap64_value(&db_key[0], &e.value_etc()[0]);  // NOLINT(readability-container-data-pointer)
    store_bswap64_value(&db_key[8], &e.value_etc()[8]);
    std::memcpy(&db_key[write_version_size], e.key_sid().data(), e.key_sid().size());
    std::string db_value(1, static_cast<char>(e.type()));
    db_value.append(e.value_etc().substr(write_version_size));
    sortdb->put(db_key, db_value);
}

static void insert_entry_or_update_to_max(sortdb_wrapper* sortdb, log_entry& e) {
    bool need_write = true;
    // skip older entry than already inserted
    std::string value;
    if (sortdb->get(e.key_sid(), &value)) {
        write_version_type write_version;
        e.write_version(write_version);
        if (write_version < write_version_type(value.substr(1))) {
            need_write = false;
        }
    }
    if (need_write) {
        std::string db_value;
        db_value.append(1, static_cast<char>(e.type()));
        db_value.append(e.value_etc());
        sortdb->put(e.key_sid(), db_value);
    }
}
```
* insert_twisted_entryは、無条件にputする。
* insert_entry_or_update_to_maxは、DBの同じキー値があるかを調べ、バージョンをチェックし、必要ならputする。

```
        epoch_id_type max_appeared_epoch = logscan.scan_pwal_files_throws(ld_epoch, add_entry);
```

* ソート処理の本体



## logscan::scan_pwal_files_throws

* 次のように定義されている。

```
epoch_id_type dblog_scan::scan_pwal_files_throws(epoch_id_type ld_epoch, const std::function<void(log_entry&)>& add_entry) {
    set_fail_fast(true);
    set_process_at_nondurable_epoch_snippet(process_at_nondurable::repair_by_mark);
    set_process_at_truncated_epoch_snippet(process_at_truncated::report);
    set_process_at_damaged_epoch_snippet(process_at_damaged::report);
    return scan_pwal_files(ld_epoch, add_entry, log_error_and_throw);
}
```

ソート処理本体は、scan_pwal_filesだった

## epoch_id_type dblog_scan::scan_pwal_files

ソースを上から呼んでいく

```
    std::atomic<epoch_id_type> max_appeared_epoch{ld_epoch};
    if (max_parse_error_value) { *max_parse_error_value = dblog_scan::parse_error::failed; }
    std::atomic<dblog_scan::parse_error::code> max_error_value{dblog_scan::parse_error::code::ok};
```

* 特にコメントなし

```
    auto process_file = [&](const boost::filesystem::path& p) {  // NOLINT(readability-function-cognitive-complexity)
        if (is_wal(p)) {
            parse_error ec;
            auto rc = scan_one_pwal_file(p, ld_epoch, add_entry, report_error, ec);
            epoch_id_type max_epoch_of_file = rc;
            auto ec_value = ec.value();
            switch (ec_value) {
            case parse_error::ok:
                VLOG(30) << "OK: " << p;
                break;
            case parse_error::repaired:
                VLOG(30) << "REPAIRED: " << p;
                break;
            case parse_error::broken_after_marked:
                if (!is_detached_wal(p)) {
                    VLOG(30) << "MARKED BUT TAIL IS BROKEN (NOT DETACHED): " << p;
                    if (fail_fast_) {
                        throw std::runtime_error("the end of non-detached file is broken");
                    }
                } else {
                    VLOG(30) << "MARKED BUT TAIL IS BROKEN (DETACHED): " << p;
                    ec.value(ec.modified() ? parse_error::repaired : parse_error::ok);
                }
                break;
            case parse_error::broken_after:
                VLOG(30) << "TAIL IS BROKEN: " << p;
                if (!is_detached_wal(p)) {
                    if (fail_fast_) {
                        throw std::runtime_error("the end of non-detached file is broken");
                    }
                }
                break;
            case parse_error::nondurable_entries:
                VLOG(30) << "CONTAINS NONDURABLE ENTRY: " << p;
                break;
            case parse_error::unexpected:
            case parse_error::failed:
                VLOG(30) << "ERROR: " << p;
                if (fail_fast_) {
                    throw std::runtime_error(ec.message());
                }
                break;
            case parse_error::broken_after_tobe_cut: assert(false);
            }
            auto tmp = max_error_value.load();
            while (tmp < ec.value()
                   && !max_error_value.compare_exchange_weak(tmp, ec.value())) {
                /* nop */
            }
            epoch_id_type t = max_appeared_epoch.load();
            while (t < max_epoch_of_file
                   && !max_appeared_epoch.compare_exchange_weak(t, max_epoch_of_file)) {
                /* nop */
            }
        }
    };
```

* 指定したパスのPWALを読むラムダ式
* ファイルがWALファイルのときだけ処理する
* WALファイルを読む処理自体は、dblog_scan::scan_one_pwal_file
* このラムダ式は、エラー処理を定義している


```
    std::mutex dir_mtx;
    auto dir_begin = boost::filesystem::directory_iterator(dblogdir_);
    auto dir_end = boost::filesystem::directory_iterator();
    std::vector<std::thread> workers;
    std::mutex ex_mtx;
    std::exception_ptr ex_ptr{};
    workers.reserve(thread_num_);
    for (int i = 0; i < thread_num_; i++) {
        workers.emplace_back(std::thread([&](){
            for (;;) {
                boost::filesystem::path p;
                {
                    std::lock_guard<std::mutex> g{dir_mtx};
                    if (dir_begin == dir_end) break;
                    p = *dir_begin++;
                }
                try {
                    process_file(p);
                } catch (std::runtime_error& ex) {
                    VLOG(log_info) << "/:limestone catch runtime_error(" << ex.what() << ")";
                    std::lock_guard<std::mutex> g2{ex_mtx};
                    if (!ex_ptr) {  // only save one
                        ex_ptr = std::current_exception();
                    }
                    std::lock_guard<std::mutex> g{dir_mtx};
                    dir_begin = dir_end;  // skip all unprocessed files
                    break;
                }
            }
        }));
    }
```
* thread_num_で指定されたスレッドを作成
* 各スレッドは、ディレクトリから一つファイルを取り出し処理する。
* 各スレッドは、処理対象のファイルがなければ、ループから抜けて処理を終了する。

## LOG 0.6対応方針

LOG-0.6では、dblog_scanの処理対象が変更になる。

* dblogutilによるコンパクション
  * コンパクションされていないPWALファイル
  * コンパクション済みファイル(※)
* 起動時のスナップショット作成
  * コンパクションされていないPWALファイル
  * コンパクション済みファイル(※)
* オンラインコンパクション
  * コンパクション済みファイル(※)
  * コンパクションされていないPWALファイル
  
※ 処理の効率化のためにコンパクション済みファイルを処理対象にせずに、利用時に既存のコンパクション済みファイルと、新規に作成したコンパクション済みファイルをマージする方法が考えられるが、現時点では実装しない。


* ~~次のメソッドで、ファイル名がから処理対象かどうかの判断を行っているが、これらだけでは判断できないのでdblog_scanの初期化時に処理対象のセットを渡しそれを処理するように変更する。~~
  * is_wal(p)
  * is_detached_wal(p)

* is_wal(p), is_detached_wal(p)をそのまま残し、現行ディレクトリから処理対象ファイルを
選ぶのではなく、dbscanの初期化時に渡した処理対象ファイルのリストから処理対象ファイルを選ぶようにする。

* dblog_scan::scan_pwal_filesの修正、