# Limestoneコード分析メモ

このドキュメントは、Limestone 0.6の設計にあたり、現行のLimestoneの仕様、実装を確認するために
ソースコードの分析を行ったさいに、記録しておくべき事項をまとめたものです。

コミット: 59dcc72 に対して分析を行った。

## 参考ドキュメント

- [資料1: データストアモジュールのI/Fデザイン](https://github.com/project-tsurugi/limestone/blob/master/docs/datastore-if.md)
  - ソースコードのどの部分をみるべきかの参考のため
- [資料2: Limestoneのリポジトリ](https://github.com/project-tsurugi/limestone/tree/master)
  - 特に明記されていない限り、6/17版(commit 49ed9d0)を参照している。


## 初期化

### 資料1:より抜粋

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

### class datastore


* `datastore::datastore(configuration conf)`
  * configで指定されたパス(data_locations_)の存在チェック
    * 存在しない場合、ディレクトリを作成
    * 必要に応じでログディレクトリを初期化
    * マニフェストファイルの作成
    * ファイルのセットを作成
      * バックアップなどで、対象ファイルのリストとして使用している模様(コメントより)
      * ログディレクトリにある、ディレクトリでないファイルとマニフェストファイル、エポックファイルが対象
      * マニフェストファイル、エポックファイルがが重に登録され得るが、セットを用いているので重複排除される。
   * コンフィグのrecover_max_parallelismの値を設定
   * datastore::recover()を呼び出す

* `datastore::~datastore()`
  * デストラクタにはなにも処理がかかれていない。

* `datastore::recover()`
  * shirakamiのstar_tup.cppのinit_body()で、datastoreを初期化(コンスとラウタの呼び出し)後に呼ばれる。
  * 引数の有無で、2つのメソッドがある。
  * ドキュメントと異なり、どちらのメソッドもready()の前に呼ばれていることのチェックのみ実行し、リカバリ処理をしていない。
  * まだ実装されていないということだと思われる。



* `datastore::ready()`
  * shirakamiのstar_tup.cppのinit_body()でadd_persistent_callback()の呼び出しの後に呼ばれる。
    * shirakamiのコメントから、ready()の前にadd_persistent_callback()を呼び出す必要がある模様
  * ログディレクトリのチェックを行った後に、datastore::create_snapshot();を呼んでいる。これが主要な処理だと思われる。

* `datastore::add_persistent_callback()`

  * epoch_id_typeを引数にとるコールバックを登録する。


* `datastore::create_snapshot()`
  * datastore_snapshot.cppに定義
  * log0.6のWAL圧縮と、同等処理がかかれていると思われる。
  * WALファイルを読み取り、snapshotファイルを作成していると思われる。
  * スナップショット関連のセクションで取り扱う。


## スナップショット関連

### 資料1:より抜粋

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


  * `datastore::create_snapshot()`
    * 


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



### `class datastore`
* `datastore::snapshot() -> snapshot`
* `datastore::shared_snapshot() -> std::shared_ptr<snapshot>`
  * どちらも、スナップショットオブジェクトを生成して返す
  * スナップショットオブジェクトの内容は、snapshotクラスを参照
* `datastore::create_snapshot()`
  * スナップショットファイルを作成する
  * スナップショット作成処理の本体
  * ここで、RocksDB(or LevelDB)のインスタンスを作成している
      ```
      #if defined SORT_METHOD_PUT_ONLY
          auto sortdb = std::make_unique<sortdb_wrapper>(from_dir, comp_twisted_key);
      #else
          auto sortdb = std::make_unique<sortdb_wrapper>(from_dir);
      #endif
      ```
  * 最大のdurable_epochを取得。
    ```
    epoch_id_type ld_epoch = logscan.last_durable_epoch_in_dir();
    ```
    * `last_durable_epoch_in_dir()`の内容は未確認
  * `insert_entry_or_update_to_max`
    * 指定のキーでDBを検索し、必要に応じてエントリをDBに登録するラムダ式
    * DBに指定のキーのエントリが存在しないか、DBに存在するエントリのバージョンより、指定のエントリのバージョンが新しいいときにDBに書き込む。
    * バージョンの比較ロジックは、`class write_version_type`に定義されている。
      * まずエポックの大小を比較し、エポックが同じ場合、バージョン番号で比較している。
        * なんか仕様と違うきがする。TODO: なぜこれで良いのか確認
  * `insert_twisted_entry`
    * insert_entry_or_update_to_max の別バージョン
    * エントリからkey, valueを作りDBにputしている
    * SORT_METHOD_PUT_ONLYが定義されているときに使用する

  * WALファイルを読んで、DBに書き込み、各種epoc_idに値をセットする。
    * 詳細は、`class dblog_scan`を読む。
      ```
      epoch_id_type max_appeared_epoch = logscan.scan_pwal_files_throws(ld_epoch, add_entry);
      epoch_id_switched_.store(max_appeared_epoch);
      epoch_id_informed_.store(max_appeared_epoch);
      ```
  * DBの内容をスナップショットファイルに書き込む(SORT_METHOD_PUT_ONLYが定義されているとき)
    * DBキーから、WriteVesrionを取り除いたキーを作成。
    * 同一のキーのデータが複数ある場合は、最初のキーのデータを処理する。
    * Valueの最初の1バイトからエントリタイプを取得
    * normal_entryの場合、スナップショットファイルに書き出す。
      * db-keyの最初の64bitと、次の64bitをstore_bswap64_valueを使用してバイトオーダ変換してスナップショットファイルに書き込む
      * db-valueの最初の1バイト(エントリタイプ)を除いた値をスナップショットファイルに書き込む
  * DBの内容をスナップショットファイルに書き込む(SORT_METHOD_PUT_ONLYが定義されていないとき)
    * db-valueの最初の1バイトからエントリタイプを取得
    * normal_entryの場合、スナップショットファイルに書き出す。
      * db-keyの値と、db-valueの最初の1バイト(エントリタイプ)を除いた値をスナップショットファイルに書き込む
  

### `class dblog_scan`

* `epoch_id_type dblog_scan::scan_pwal_files_throws`
  * fail_fast_ に true をセット
  * process_at_nondurable に repair_by_mark をセット
  * process_at_truncated_ に report をセット
  * process_at_damaged_ に report をセット
  * `epoch_id_type dblog_scan::scan_pwal_files`を呼び出す
  * 以上
  
* `epoch_id_type dblog_scan::scan_pwal_files`  
  * 引数:
    * ld_epoch: エポックの最大値
    * add_entry: エントリを処理するためのコールバック関数です。
    * report_error: エラーが発生した際に報告するための関数です。
    * max_parse_error_value: 最大のパースエラー値を返すための出力するためのポイント(省略可能)
  * 戻り
    * スキャンしたログファイルに現れた最大エポック
  * max_appeared_epoch に ld_epoch の値をセット
  * max_error_value に ok をセット
  * ラムダ式 `process_file`
    * p: 処理対象のパス
    * ファイル名から p が PWALファイルであると判断したときのみ処理を実行する。
    * `dblog_scan::scan_one_pwal_file`を呼び出す。
      * PWALファイルを読み、エントリを書き出す
      * 当該ファイルの最大エポックを返す
      * ecにエラー情報が入る
    * ecによる処理分岐
      * parse_errorの値に応じた処理
        * ok, repaired, broken_after_tobe_cut, broken_after_marked, nondurable_entries
          * 値に応じたログを出力し、処理を継続
        *  broken_after, unexpected, failed
          * 値に応じたログを出力し、エラーをスローする
  * 複数のスレッドを作成してディレクトリ内のファイルを並行して処理し、例外が発生した場合にはすべてのスレッドが安全に停止する
  * 全てのPWALファイルのエポックの最大値がmax_appeared_epochに格納されれ、この値を戻り地として返す。

* `dblog_scan::scan_one_pwal_file`
  * 引数
    * p: 解析対象のWALファイルのパス
    * ld_epoch: 最大エポック
    * add_entry: WALファイルから解析したエントリを追加するためのコールバック関数
    * report_error: 解析中に発生したエラーを報告するための関数
    * pe: パースエラーをキャプチャして報告するための parse_error オブジェクトへの参照
  * 戻り
    * 当該ファイルの最大エポック
  * ローカル変数
    * current_epoch: 現在処理中のエポック
    * max_epoch_of_file: とファイル内で見つかった最大のエポック
    * ec: ログエントリを読み取る際に発生したエラー
    * fixed: 処理中に修正されたエラーの数
    * e: ログエントリ
    * fpos_epoch_snippet: ストリーム上の位置
  * `err_unexpected`
    * エラーレポート用のラムダ式
  * p から入力ストリームを作成する
  * フラグの設定、marked_before_scanはfalseが設定される。
    ```
    bool valid = true;  // scanning in the normal (not-invalidated) epoch snippet
    [[maybe_unused]]
    bool invalidated_wrote = true;  // invalid mark is wrote, so no need to mark again
    bool marked_before_scan{};  // scanning epoch-snippet already marked before this scan
    bool first = true;
    ```
  * ec に ok をセット
  * 以下の処理を無限ループ
  * fpos_before_read_entry にストリームの現在位置をセット
  * `log_entry::read_entry_from` でストリームからエントリを読み取り、戻り値を data_remains にセットする。
    * data_remains
      * true: 正常に読めた
      * false: エラーが発生(ストリームの終端に達した場合を含む)
  * ec, data_remains, e から tokの値を決める -> `token_type`
  * tok による処理の分岐
  * TODO: 詳細は後で読む
  * eofになれば無限ループから抜ける
    * ストリームをクローズ
    * max_epoch_of_fileを返す

### `class log_entry`


* `log_entry::read_entry_from`
  * 引数
    * strm
      * 入力ストリーム
    * ec
      * エラーコードの設定先
  * 戻り値(bool)
    * true: 成功
    * false: エラー => エラーコドがecにセットされる。
  * 処理内容
    * ecにデフォルト値をせってエイ
    * ストリームから1バイト読み込む
      * 読めなかったら、エラー終了、ecは前に設定したデフォルト値
    * 1バイト読んだ値からエントリタイプが決まる。エントリタイプに応じた処理をする。
    * `entry_type::normal_entry`
      * ストリームから32bit読み取りkey_lenにセットする。
      * ストリームから32bit読み取りvalue_lenにセットする。
      * ストリームからkey_len + sizeof(storage_id_type)で指定したバイト数読み込み、key_sid_にセットする。
      * ストリームからvalue_len + sizeof(epoch_id_type) + sizeof(std::uint64_t)で指定したバイト数読み込み、value_etc_にセットする。
      * 指定のバイト数読めないときはhort_entryのエラーになる。
    * `entry_type::remove_entry`
      * ストリームから32bit読み取りkey_lenにセットする。
      * ストリームからkey_len + sizeof(storage_id_type)で指定したバイト数読み込み、key_sid_にセットする。
      * ストリームからsizeof(epoch_id_type) + sizeof(std::uint64_t)で指定したバイト数読み込み、value_etc_にセットする。
      * 指定のバイト数読めないときはhort_entryのエラーになる。
    * `entry_type::marker_begin`, `entry_type::marker_end`, `entry_type::marker_durable`, `entry_type::marker_invalidated_begin`
      * ストリームから64bit読み取りepoch_id_にセットする。
      * ecを調べ、エラーが起きている場合は終了する。
        * 64bit読めないとshort_entryのエラーになる。
    * その他
      ```
      ec.value(read_error::unknown_type);
      ec.entry_type(entry_type_);
      return false;
      ```


### `class lex_token`

* `token_type`
  * eof: ファイルの終端
  * 正常なエントリ
    * normal_entry = 1: 
    * marker_begin: 
    * marker_end: 
    * marker_durable: 
    * remove_entry: 
    * marker_invalidated_begin: 
  * ショートエントリ
    * SHORT_normal_entry = 101: 
    * SHORT_marker_begin: 
    * SHORT_marker_end: 
    * SHORT_marker_durable: 
    * SHORT_remove_entry: 
    * SHORT_marker_inv_begin: 
  * UNKNOWN_TYPE_entry = 1001: 
* コンストラクタの引数
  * ec
    ```
          enum code {
            ok = 0,
            // warning
            nondurable_snippet = 0x01,
            // error
            short_entry = 0x81,
            // unknown type; eg. type 0
            unknown_type = 0x82,
            // unexpected type; eg. add_entry at the head of pwal file or in epoch file
            unexpected_type = 0x83,
        };
    ```
  * data_remains
    * true: 正常に読めた
    * false: エラーが発生(ストリームの終端に達した場合を含む)
  * e: ログエントリ
* コンストラクタの引数、ec, data_remains, e からtoken_typeを決定する。
  * ec.value() == 0 の場合
    * eof: ファイルの終端に達した
    * e.type()に応じた正常なエントリを設定
  * ec.value() == short_entry の場合
    * e.type()に応じたショートエントリを設定
  * ec.value() == unknown_type の場合
    * UNKNOWN_TYPE_entry



### `class write_version_type`

次のフィールドをおつ
* epoch_number_
  * エポック番号
* minor_write_version_
  * 同一エポック内でのバージョン番号
* 大小比較
  * エポック番号を比べる
  * エポック番号が同じならバージョン番号を比べる



### `class sortdb_wrapper`

* RocksDB または LevelDB を用いたソート機能を提供する。

* コンストラクタ `sortdb_wrapper`
  * `dir`パラメータで指定されたディレクトリに、`sortdb_wrapper`にデータベースファイルを作成する。
  * keycomp はオプションで、ユーザー定義の比較関数を提供するためのものでソート時に使用される。
    * DB作成時のオプションとして渡される。コンストラクタに指定されていない場合は、オプションが指定されないのでDBのデフォルトのコンパレータを用いるものだと思われる。
* デフォルトコンストラクタ、コピーコンストラクタ、コピー代入演算子、ムーブコンストラクタ、ムーブ代入演算子を無効化
  ```
  sortdb_wrapper() noexcept = delete;
  sortdb_wrapper(sortdb_wrapper const& other) noexcept = delete;
  sortdb_wrapper& operator=(sortdb_wrapper const& other) noexcept = delete;
  sortdb_wrapper(sortdb_wrapper&& other) noexcept = delete;
  sortdb_wrapper& operator=(sortdb_wrapper&& other) noexcept = delete;
  ``` 

* `put`, `get`
  * DBに対するput/getを行う。
* `each(const std::function<void(std::string_view, std::string_view)>& fun)`
  * DBの全てのエントリに対して、引数funで指定された操作を行う。
  * `datastore::create_snapshot()`でDBの内容をスナップショットファイルに書き出すために使用されている。



###  `class snapshot`

* 現状の実装では、スナップショットファイルを読むためのcursorクラスのオブジェクトを
返す機能のみを持つ。
* コンストラクタで渡されたスナップショットファイルのパスからストリームを作成して、cursorに渡している。
* snapshotの作成処理は、即ちこのスナップショットファイルの作成処理である。

> スナップショットオブジェクトが有効である限り、当該スナップショットから参照可能なエントリはコンパクションによって除去されない
  
* 上の要件は、まだコンパクション機能がないため、満たされている。
* thread safeが実現されているかよくわからないが多分OK
  * 現状は起動時にしか使用されず、スレッドセーフでなくても問題ないかもしれない
  * カーソルクラスはスレッドアンセーフ、スレッドごとにカーソルを持つという設計のようだ。
  * そもそも、ほとんど実装されていない。

* find()とscan()は未実装、get_cursor(), file_path()のみ実装済み。
* file_path()が何に使われるか不明
  * get_cursor()で、ファイルのパスを探すのだけに使用(praivateだった)


### `class cursor`

* 非常に単順
* ファイルをシーケンシャルに読んでいるだけ
* 実質的な実装は、log_entryクラスにある。
* log_entryクラスのドキュメント欲しい、
  * これスナップショットファイルの仕様だよね。
  * これ、スナップショットだけでなく、WALのRead/Writeにも使われていると思うので、WALの仕様でもある。
  



## データ投入

### 資料1:より抜粋

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

### `class datastore`

* `datastore::create_channel(path location) -> log_channel`
  * overview
    * ログの出力先チャンネルを追加する
  * param `location`
    * ログの出力先ディレクトリ
  * limit
    * この操作は `ready()` が呼び出される前に行う必要がある
  * チャネルとログのロケーションのパスが紐付いていた。
    * 引数でローケーションが渡されている。
  * ログチャネルのIDを採番し、ログチャネルのオブジェクトを生成、ログチャネルを管理するオブジェクトに登録し、再生したログチャネルを返す。
  * ログチャネルのコンストラクタは、ログファイルのファイル名を生成しているだけだった。



* `datastore::last_epoch() -> epoch_id_type`
  * 内部変数 epoch_id_informed_ の値を返しているだけ


* `datastore::switch_epoch(epoch_id_type epoch_id)`
  * overview
    * 現在の epoch ID を変更する
  * note
    * `datastore::ready()` 呼び出し以降に利用可能
    * epoch ID は前回の epoch ID よりも大きな値を指定しなければならない
  * プライベートフィールド: epoch_id_switched_の値を更新する。似たようなフィールドに epoch_id_informed_, epoch_id_recorded_がある。
    * epoch_id_recorded_はエポックファイルに記録されたエポックIDだと思われる。
    * epoch_id_informed_は、`datastore::last_epoch()`で参照するエポックIDでもある。
      * epoch_id_recorded_ と epoch_id_recorded_ は、`datastore::update_min_epoch_id(bool from_switch_epoch)` で更新される。
      * `datastore::update_min_epoch_id(bool from_switch_epoch)`は、このメソッド: `datastore::switch_epoch(epoch_id_type epoch_id)`と、`log_channel::end_session()`から呼ばれている。
      * update_min_epoch_id は、次の処理を行っている。
        * 各ログチャネルを調べて、各ログチャネルのfinished_epochの最大値をmax_finished_epochに設定
        * さらに各ログチャネルのworking_epoch - 1の値の最小値をupper_limitに設定。
          * 各ログチャネルが書き込み中、または書き込みが完了している。epchoのうち、最も小さいもの -1がupper limit
        * epoch_id_recorded_を更新する。
          * datastore::switch_epochから呼ばれているときは、max_finished_epochとupper_limitのうちの小さい方の値
          * それ以外の場合は、upper_limitの値。
          * このようにして決めた値が、現在の epoch_id_recorded_ より大きい場合に、epoch_id_recorded_を更新する。
        * epoch_id_informed_を更新する
          * upper_limitの値に更新する。
    * まとめ
      * 現在ログチャネルのエポックについて
        * upper_limit
          * 各ログチャネルの処理中のエポックの中で最小の値 - 1
          * 全てのログチャネルのセッションが非アクティブな場合、epoch_id_switched_ - 1
        * finished_epoch
          * 各ログチャネルで処理が完了したエポックの中で最大の値
      * epoch_id_switched_
        * datastore::switch_epoch(epoch_id_type new_epoch_id)で指定されたエポック
      * epoch_id_recorded_
        * 永続化が完了したエポック、値は次のようにして決められる。
        * `datastore::switch_epoch(epoch_id_type epoch_id)` から呼ばれた時
          * upper_limit と finished_epoch のうち小さい方の値
        * `log_channel::end_session()`から呼ばれた時
          * upper_limit
      * epoch_id_informed_
        * この値を入れてpersistent_callback_を呼ぶ
          * 複数のログチャネルが異なるepcohを書き込み中の場合、最小値
          * 全てのログチャネルのセッションが非アクティブな場合、epoch_id_switched_ - 1の値。



* `datastore::add_persistent_callback(std::function<void(epoch_id_type)> callback)`
  * overview
    * 永続化に成功した際のコールバックを登録する
  * note
    * この操作は、 `datastore::ready()` の実行前に行う必要がある
  * 補足事項なし

* `datastore::switch_safe_snapshot(write_version_type write_version, bool inclusive)`
  * overview
    * 利用可能な safe snapshot の位置をデータストアに通知する
  * note
    * `datastore::ready()` 呼び出し以降に利用可能
    * write version は major, minor version からなり、 major は現在の epoch ID 以下であること
    * この操作の直後に当該 safe snapshot が利用可能になるとは限らない
    * `add_safe_snapshot_callback` 経由で実際の safe snapshot の位置を確認できる
    * `datastore::ready()` 直後は `last_epoch` を write major version とする最大の write version という扱いになっている
  * 現在未実装

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
  * コールバックの登録が行われている。コールバックを呼ぶ処理は未実装に見える。


### `class log_channel`
* `log_channel::begin_session()`
  * overview
    * 現在の epoch に対する永続化セッションに、このチャンネルで参加する
  * note
    * 現在の epoch とは、 `datastore::switch_epoch()` によって最後に指定された epoch のこと
  * current_epoch_id_に現在のepochを設定し、WALファイルをオープンする。
  * datasstoreにWALファイルが未登録の場合登録する。
  * セッション開始のログを書き込む
  * 疑問: グループコミットが行われてepochより、current_epoch_id_が大きくなることがあり得ると思うのだが、それで良いのか(そういう設計なのか)

* `log_channel::end_session()`
  * overview
    * このチャンネルが参加している現在の永続化セッションについて、このチャンネル内の操作の完了を通知する
  * note
    * 現在の永続化セッションに参加した全てのチャンネルが `end_session()` を呼び出し、かつ現在の epoch が当該セッションの epoch より大きい場合、永続化セッションそのものが完了する
  * ファイルをフラッシュし、syncする
  * finished_epoch_id_にcurrent_epoch_idを設定する
  * datastore::update_min_epoch_id(false)を呼び出す。
    * エポックファイルのepoch_idを更新するためだと思う。
  * ファイルをクローズする。




* `log_channel::abort_session(error_code_type error_code, std::string message)`
  * overview
    * このチャンネルが参加している現在の永続化セッションをエラー終了させる
  * 現状未実装

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
  * write_version は省略可能とあるが、ソースコードからは省略可能にみえない。
  * large_objects を指定可能なメソッドは未実装。


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
  * 現状未実装


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
  * 現状未実装


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
  * 現状未実装

## バックアップ

### 資料1:より抜粋

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

### `class backup`

バックアップ操作をカプセス化するクラス
- **`explicit backup(std::set<boost::filesystem::path>& files) noexcept`**
  - 指定されたファイルセットを使用して `backup` オブジェクトを初期化します。

- **`[[nodiscard]] bool is_ready() const noexcept`**
  - バックアップ操作が利用可能かどうかを確認します。
  - 現在未実装で、常にtrueを返す

- **`[[nodiscard]] bool wait_for_ready(std::size_t duration) const noexcept`**
  - バックアップ操作が利用可能になるまで指定された時間だけ待ちます。
  - 現在未実装で、常にtrueを返す

- **`std::vector<boost::filesystem::path>& files() noexcept`**
  - バックアップ対象のファイルリストを返します。
  - コンストラクタで指定したファイルのリストを返す



### `class datastore`

* `std::unique_ptr<backup_detail> datastore::begin_backup(backup_type btype)`
  * 引数: backup_type::standard　or backup_type::transaction
  * 戻り値: backup_detail
  * 最初に`rotate_log_files()`を呼びログをローテートする
    * これにより、アクティブなファイルは全てローテートされるので、非アクティブなファイルがバックアップ対象になる。
  * datastoreのフィールドfiles_にdatastoreが管理するファイル。具体的にはPWALファイルとエポックファイルが含まれる。
  * files_からアクティブなファイルを除いたinactive_filesを作成する。
    * アクティブなファイルとは、現在のログチャネルが保持しているPWALファイルと、datastoreが現在使用しているエポックファイルfiles_からアクティブなファイルを除いた
    * inactive_filesからエントリを作成する。
      * ファイル名から、ファイルのタイプを判断する
        * PWAL
        * エポックファイル
        * マニフェストファイル
      * エントリ
        * source_path_　=> 完全なパス
        * destination_path_=> ファイル名
        * is_mutable_ => マニフェストファイルのときだけ　true
        * is_detached_=> 常にfalse
  * このメソッドがスレッドセーフでないが、問題ないのか

* `datastore::restore(std::string_view from, bool keep_backup) -> restore_result`
  * overview
    * データストアのリストア操作を行う
    * keep_backupがfalseの場合は、fromディレクトリにあるWALファイル群を消去する
  * note
    * この操作は `datastore::ready()` 実行前に行う必要がある
    * `LOG-0`のリストア操作は、fromディレクトリにバックアップされているWALファイル群をlogディレクトリにコピーする操作となる  
  
* `status datastore::restore(std::string_view from, std::vector<file_set_entry>& entries)`
  * 指定されたディレクトリから、提供されたエントリ情報に基づいてファイルを復元します。
  * 引数
    * from: バックアップファイルが格納されているディレクトリのパス。
    * entries: 復元するファイルの詳細情報を持つ file_set_entry オブジェクトのベクタ。
  * 戻り値: 復元操作の成否を示す status オブジェクト。
  * entriesからマニフェストファイルを探し、マニフェストのバージョン違いなどの問題ないかをチェックする。
    * 問題がある場合は、エラーを返す。
    * 問題がない場合は次に進む
  * リストア先のディレクトリ、location_をパージする。
  * entries をループし、各ファイルを from ディレクトリから location_ ディレクトリにコピーします。

#### class backup_detail

* `class entry`
  * バックアップファイルを表す内部クラス
  * 以下のフィールドを持つ
    * boost::filesystem::path source_path_
    * boost::filesystem::path destination_path_
    * bool is_mutable_
    * bool is_detached_
* コンストラクタ
  * このオブジェクトの生成は、`datastore::begin_backup`で行っているので、そちらを参照すべし。
* フィールド
  * std::string_view configuration_id
    * バックアップのID
  * epoch_id_type log_finish
    * このバックアップの最大エポックID
  * std::vector<backup_detail::entry> entries_
    * このバックアップに含まれるファイルの一覧
* メソッド
  * `bool is_ready() const`
    * このオブジェクトが利用可能かを返す
    * 現在の実装では、オブジェクトの生成時ににオブジェクトが利用できるようになるのを待ち、このメソッドは常にtrueを返す。
    * 
* `epoch_id_type log_start()`
  * 最小のエポックIDを返す。
  * 現実装では常に0を返す。
* `epoch_id_type log_finish()`
  * 最大のエポックIDを返す
* `std::optional<epoch_id_type> image_finish() `
  * 未実装
* `std::vector<backup_detail::entry>& entries()` 
  * エントリのリストを返す
* `std::string_view configuration_id()`
  * コンフィグレーションIDを返す

### 世代管理

* スケルトンが存在するのみで、未実装

### 進捗確認

* スケルトンが存在するのみで、未実装



## 2024-07-04版の解析メモ

主にログコンパクションツール周りについて

コンパクション機能追加のコミットで修正が入ったのは次のファイルなので
これらのファイルについて見ていく

```
umegane@ubuntu22:~/git/limestone$ git diff --name-only 38ad7d4 24f2611
src/limestone/datastore_snapshot.cpp
src/limestone/dblog_scan.h
src/limestone/dblogutil/dblogutil.cpp
src/limestone/internal.h
```

### internal.h

次のメソッドが追加されていた

```
void create_comapct_pwal(const boost::filesystem::path& from_dir, const boost::filesystem::path& to_dir, int num_worker);
```

### dblogutil.cpp

* 主要な追加メソッド
  * `void compaction(dblog_scan &ds, std::optional<epoch_id_type> epoch) `
  * 詳細後述
* その他の追加メソッド
```c++
static boost::filesystem::path make_tmp_dir_next_to(const boost::filesystem::path& target_dir, const char* suffix) {
    auto tmpdirname = boost::filesystem::canonical(target_dir).string() + suffix;
    if (::mkdtemp(tmpdirname.data()) == nullptr) {
        LOG_LP(ERROR) << "mkdtemp failed, errno = " << errno;
        throw std::runtime_error("I/O error");
    }
    return {tmpdirname};
}

static boost::filesystem::path make_work_dir_next_to(const boost::filesystem::path& target_dir) {
    // assume: already checked existence and is_dir
    return make_tmp_dir_next_to(target_dir, ".work_XXXXXX");
}

static boost::filesystem::path make_backup_dir_next_to(const boost::filesystem::path& target_dir) {
    return make_tmp_dir_next_to(target_dir, ".backup_XXXXXX");
}
``` 
* その他
```c++
DEFINE_bool(force, false, "(subcommand compaction) skip start prompt");
DEFINE_bool(dry_run, false, "(subcommand compaction) dry run");
DEFINE_string(working_dir, "", "(subcommand compaction) working directory");
DEFINE_bool(make_backup, false, "(subcommand compaction) make backup of target dblogdir");
```

#### compaction()

```c++
void compaction(dblog_scan &ds, std::optional<epoch_id_type> epoch)
```
* パラメータ
  * dblog_scan
    * データベースログをスキャンするためのオブジェクトの参照
  * epoch
    * エポックID
    * このエポックIDまで処理する
      * TODO: 処理するとは？
* エポックIDの決定
  * 引数で指定された場合、その値
  * 引数で指定されない場合は、エポックファイルからlast_durable_epochを取得する
* ワークディレクトリの準備
  * コマンドライン引数でワークディレクトリが指定されている場合、そのディレクトリ
  * 指定されていない場合、ログディレクトリと同階層のワークディレクトリを作成して使用する
* --forceオプションがないときの処理
* `setup_initial_logdir()`
  * ワークディレクトリの初期化
  * logdirの初期化に使用しているのと同じメッソドを呼び出している
  * マニフェストファイルを作成している。
* `create_comapct_pwal()`
  * コンパクション処理の本体、後述
* エポックファイルの書き出し
* コマンドライン引数でバックアップが指定されている場合
  * 既存のログディレクトリをバックアップディレクトリにリネームする。
  * そうでない場合は既存のログディレクトリを削除する。
* ワークディレクトリを既存のログディレクトリのパスにリネームする。


#### create_comapct_pwal()

```c++
void create_comapct_pwal(const boost::filesystem::path& from_dir, const boost::filesystem::path& to_dir, int num_worker)
```

* パラメータ
  * 元ディレクトリ
  * 先ディレクトリ
  * ワーカースレッド数
* `create_sortdb_from_wals`
  * ソート済みのDBを作成する。
  * 以前のコミットの`create_snapshot()`に相当するが、いろいろ変わっている。
  * ソート済みのDBとエポックIDの最大値が戻る
  * 詳細後述
* to_dirにコンパクト化されたログファイル（pwal_0000.compacted）を作成する。
  * 最初にセッション開始のエントリを書く
  * ソート済みDBの各エントリに対する処理
    * 通常エントリのみが処理対象、削除エントリはスキップする
      * フルコンパクションの場合、削除エントリはスキップで良いが、差分コンパクションの場合残さないといけない。
    * rewind flagに応じて、どちらかの処理を行う(現状 rewind = true固定)
    * rewind = true
      * DNから取り出したエントリの valueの先頭16バイトを0埋めする
      * エントリのkeyと先頭16バイトを0埋めしたvalueを書き出す
    * rewind = false
      * DBから取り出したエントリの key と value をそのまま書き出す
  * 最後にセッション終了のエントリを書く => 現状コメントアウト