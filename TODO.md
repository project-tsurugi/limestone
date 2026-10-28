# TODO: R1 ソート方式の置き換え (作業計画)

作業ブランチで使う計画と進捗のメモ。作業しながら更新し、各コミットに含める。作業の記録としてブランチのコミットにだけ残し、
master へのマージ前に本ファイルを削除する。

## 1. 概要

* 目的: 起動時のスナップショットファイルの作成とコンパクションのソートを、RocksDB からインメモリソートとマージソートの
  組み合わせに置き換え、RocksDB・LevelDB への依存をなくす。あわせて log_entry を再設計する (ソートの前提になるため)。
* 設計の正本
  * L3 (全体): 8 節 (ソート方式)、13 節 (測定)、15 節 (互換性)
  * log_entry 再設計 仕様 (以下 LE): 9〜14 節 (設計)、15 節 (テスト観点)、16 節 (移行の段階)
  * S1 メモ: 2 節 (決定事項)、4 節 (環境変数の既定値)、5 節 (変更箇所)、6 節 (テスト項目)、7 節 (作業順序)、
    8 節 (limestone 以外の対応)
  * L3 は、レビュー対応 (nt-tsurugi-internal の instant-recovery-L3 ブランチ 7de8a93、2026-10-23) の時点のものを正本と
    する。S1 メモ 4 節の環境変数の名前は、L3 12.5 節の設定項目に読み替える (5 節の 2026-10-26 の記録)。
  * ディレクトリの fsync の検討 (nt-tsurugi-internal の 09-notes/directory-fsync-notes.md。以下 DF): ステップ 13 の対象。
    L3 (全体) 12.2 節・17 節の申し送り事項の検討
* 基準コミット: 48032ee (LE と S1 メモの行番号はこのコミットのもの)
* 日程 (WBS): 2026-10-20〜2026-11-02 (10 人日)。

## 2. 作業の進め方

* 各ステップの終わりで、ビルドが通り、既存のテストが通る状態にする (LE 16.2)。
* 修正の都度のテストは対象のテストだけを filter で実行する。フルスイートはステップの区切りとコミット前に実行する。
* コメントは実装中は日本語で書き、コミット準備で英訳する。
* コミットメッセージは Conventional Commits の英文とし、案に和訳を添えて確認を得てからコミットする。
* 計画からの変更、判断した事項、残った課題は本ファイルの 5 節・6 節に記録する。

## 3. 着手前の確認事項

| # | 事項 | 状態 |
|---|---|---|
| Q1 | 作業ブランチ名 | `feat/replace-recovery-sorter` に決定 |
| Q2 | 先行修正 (ステップ 1) を作業ブランチに含めるか、master に別に入れるか | 作業ブランチに含める |
| Q3 | PR の単位。R1 全体で 1 本にするか、ソートの置き換え (ステップ 1〜8) と LE の段階 2〜4 (ステップ 10〜12) に分けるか | R1 全体で 1 本 |
| Q4 | 公開 API `write_version_type(const std::string&)` と `write_version_type(std::string_view)` の削除 (LE 10.4、段階 4)。公開 API は参照がなくても削除しない方針との関係。LE で削除と決めているが、改めて確認する | 削除する (LE 10.4 のとおり) |
| Q5 | 段階 1 で所有型を 1 本のバッファに変えると、`const std::string&` を返す旧アクセサ (`key_sid()`、`value_etc()`、`raw_blob_ids()`、log_entry.h:463-471) を段階 4 まで残せない。戻り値の型を先に変えるか、所有型の構成の変更を段階 2〜3 へ回すか | 段階 1 で旧アクセサの戻り値を string_view に変える。名前は段階 4 まで旧名のまま残す (LE 10.3 の戻り値の型の変更の前倒し) |
## 4. ステップ

### ステップ 0: 準備

* [x] 作業ブランチを作る (Q1)。
* [x] 現状のビルドとフルスイートの結果を記録する (基準)。5 節に記録した。
* [x] 旧バージョンとの出力の比較 (ステップ 8) に使う、旧バージョン (既定の構成、RocksDB・PUT_ONLY) のビルドを用意しておく。
      Tsurugi のリリースビルドを使うので、特別な作業は不要とした。

### ステップ 1: 先行修正 (LE 16.1)

* [x] P4: `test/limestone/blob/log_entry_comparator_test.cpp` の誤った前提 (「storage_id が小さいほど key_sid が辞書順で
      小さい」) のコメントと検証を直す。2 バイト以上の storage_id で成り立たないことを確かめる形にする。
      既存のテストは 1 バイトのケースとして残してコメントを直し、2 バイト以上のテストを 4 件追加した。
* [x] P14: `dblogutil` の count-durable-wal-entries に normal_with_blob とストレージ系を加える (dblogutil.cpp:85-93)。
      6 種を列挙して数え、それ以外は従来どおり LOG(ERROR)。UT を 1 件追加した。

### ステップ 2: log_entry の基盤 (LE 段階 1 の前半)

* [x] 形式の定義のヘッダ (log_entry/log_entry_format.h): 各フィールドの長さの定数、形式の記述、key_sid の順序の
      比較関数 (LE 10.1・10.2)。static_assert (LE 10.5)。シリアライズとデシリアライズは参照型と所有型に置く (5 節)。
* [x] 参照型 (log_entry/log_entry_view.h の log_entry_view) と解析関数 (LE 9.3・11.1)。不足のときの下限の計算を含む。
      分類の判定関数 (LE 9.1) と参照型のアクセサの種別の検査 (LE 9.2) を前倒しで入れる (5 節)。
* [x] 所有型 log_entry の構成を 1 本のバッファに変え、アクセサを参照型に委ねる (LE 9.3)。
      move 元が既定構築の状態になること。
  * [x] 旧アクセサ key_sid()・value_etc()・raw_blob_ids() の戻り値を const std::string& から string_view に変える (Q5)。
        コンパイルが通らなくなる呼び出し元を直す: blob_file_gc_snapshot.cpp:97、datastore_snapshot.cpp の旧ソート
        (ステップ 7 で削除するまでのつなぎ)、test/limestone/blob/log_entry_blob_test.cpp:224。
* [x] 最も古い版の write_version を返す関数 oldest_write_version() (LE 13.2。定数ではなく関数にした経緯は 5 節)。
* [x] ファクトリ (LE 13.3)。データ系・ストレージ系・marker の各種別。長さの違反で abort。
* [x] write_serialized と、最も古い版に置き換えて書く指定 (LE 12)。
* [x] UT: LE 15 節の解析関数、比較関数、所有型、write_serialized の観点。

### ステップ 3: ファイルからの読み込み (LE 段階 1 の後半)

* [x] POSIX の read による読み込み (LE 11.2)。バッファの補充の 3 分岐、位置を指定した読み始め、終端と short_entry の区別。
* [x] 書き込み中のファイルの読み出し (LE 11.5)。状態の関数、待ち方。
* [x] parse_wal_file と dblog_scan の読み込みを新しい読み込みに移す。無効なスニペットの marker の書き換えは、別に開いた
      ファイルへの位置を指定した書き込みに変える (LE 11.2)。失敗時の判定を read_error の種別に変える (LE 11.3)。
* [x] UT: LE 15 節のファイルからの読み込み、書き込み中のファイルの読み出しの観点。項目ごとに書いた (5 節に対応表)。
* [x] 既存の dblog_scan・parse_wal_file・compaction のテストが変えずに通ること。25c5852 のフルスイートで確認した。

### ステップ 4: 新しいソートの部品 (L3 8.2〜8.6)

* [x] バッファ: 固定長のアリーナとインデックスの配列。満杯の判定、アリーナを超えるエントリの単独書き出し (8.3)。
* [x] インデックスに対する std::sort。比較はアリーナ内の key_sid と write_version を直接読む (8.2・8.3)。
* [x] 縮約: ソート済みの流れから「直前に出した key_sid と同じエントリ」を除く。バッファの書き出しとマージの出力で同じ
      ものを使う (key_sid_deduplicator)。
* [x] ソート済みファイルの書き出しと読み込み (LE 14)。末尾で不足と不正は limestone_io_exception。
* [x] 置き場所 sorting の作成と削除。親ディレクトリは設定項目 (sort_work_dir。L3 12.5)、未指定はログディレクトリ (8.5)。
* [x] マージ: マージの上限 (設定項目 sort_merge_fanin、既定 128) による中間のマージと最後のマージ。ソート済みファイルが
      2 以上かつ上限以下なら 1 段。0 個・1 個のときも最後のマージと同じ処理で出力を作る (入力は空またはその 1 個。
      コンパクションでは現行世代のコンパクション済み WAL ファイルを加える)。どのマージでも縮約し、
      範囲削除の適用は最後のマージだけで行う。マージは 1 本のスレッド (8.6、S1 メモ 2 節)。
* [x] 最後のマージの結果を参照型でコールバックに渡す (S1 メモ 2・5 節)。
* [x] コンパクションの最後のマージの入力に現行世代のコンパクション済み WAL ファイルを加える。先頭の marker_begin を読み飛ばし、
      normal_with_blob を BLOB の GC のために記録する (8.6)。
* [x] ソートの設定値 (スレッド数、アリーナの大きさ (MiB)、マージの上限、置き場所の親ディレクトリ) を、ソートの部品が
      引数で受け取る形にする。既定値はスレッド数 8、アリーナ 256 MiB (S1 メモ 4 節)、マージの上限 128、置き場所は
      ログディレクトリ (L3 12.5)。実行ファイルごとの指定の手段 (tsurugi.ini、オプション、環境変数) はステップ 6。
* [x] テストからマージの上限とアリーナの大きさを小さく指定する手段 (sort_buffer と sorted_file_merger の引数)。

### ステップ 5: dblog_scan の拡張 (L3 8.4、S1 メモ 5 節)

* [x] スレッドの終了時のフック (そのスレッドのバッファの残りをソートして書き出す)。
* [x] dblog_scan のスレッドをソートのスレッドとし、add_entry が呼ばれたスレッドのアリーナにコピーする (走査と同じ
      スレッド。参照型は add_entry から戻るまで有効なので同期は不要。L3 8.4、S1 メモ 5 節)。full ならそのスレッドで
      sort() → sorted_file_writer で書き出し → clear() → 入れ直し。oversized なら 1 件だけを単独のファイルへ。
      (スレッド 1 本分を sort_worker にまとめた。add_entry とフックへの配線は次の駆動の項目。)
* [x] ソート全体の駆動: sorting_directory の作成、スレッドごとの sort_buffer と writer、書き出したソート済みファイルの
      一覧の収集、走査の後の merger の呼び出し。走査が失敗したらマージを行わない。ステップ 6 の呼び出し元はこれを呼ぶ。
      (wal_sorter。scan() と merge() の 2 段階。範囲削除と BLOB ID の最大値は wal_sorter が記録する。)
* [x] UT (ソート全体の総括。部品ごとの UT はステップ 4 で済み): 順序と縮約、範囲削除の適用、多段のマージ、
      ソート済みファイルが 0 個・1 個のときの出力、アリーナを超えるエントリ、sorting の作成と削除 (S1 メモ 6 節)。
* [x] 入力のファイルを、閉じられるまで待つキューから取り出す形にする。起動時とコンパクションでは、すべてのパスを入れてから閉じる。
      (input_file_queue。失敗したワーカーもキューを中断して他を止める。中断の後の push は捨てる。)
* [x] キューの中断 (失敗時。スレッドは読みかけのファイルを打ち切って終了する)。
      (走査の入力を scan_input にし、parser はエントリごとに stopped() を見る。投入側の中断は scan_pwal_files の例外に。)
* [x] UT: キューを閉じたときと中断したときの動作、失敗の伝え方。(dblog_scan_test の 3 件。sleep によるタイミング依存のテストは置かない。)

### ステップ 6: 呼び出し元の置き換え (S1 メモ 5 節)

* [x] create_snapshot_and_get_max_blob_id(): 新しいソートに置き換える。remove_entry は、コンパクションカタログに
      コンパクション済み WAL ファイルがあるときだけ書く。
      (走査の共通部分を scan_wals() にした。wal_sorter::scan() の引数は境界の epoch と、超えたスニペットの扱い
      (beyond_boundary_handling) の 1 組にまとめた。5 節。)
* [x] create_compaction_output(): 新しいソートに置き換え、write_serialized の最も古い版に置き換える指定で書く。
      remove_entry は書かない。値の 1 件ごとのコピーをなくす。
      (使われなくなった旧ソートの関数は、ここで前倒しして削除した。ステップ 7 を参照。)
* [x] 上の 2 つの出力の書き出しを sorted_file_writer と共通化するかを、置き換え時に判断する。違いは先頭の marker_begin の
      有無、write_version の置き換え、remove_entry の扱いの 3 つで、writer の引数にできる。既存の経路の一時ファイルと
      rename、失敗時の削除との兼ね合いを見比べて決める (2026-10-26 のレビューでの気づき)。
      (出力は別クラス sort_output_writer にし、stdio の扱いだけを buffered_output_file に切り出して共有した。5 節。)
* [x] BLOB の ID の最大値を、出力からではなく走査の中で記録する (縮約と範囲削除で除くエントリの分も含める。L3 8.1)。
      tsurugi-issues #1590 の報告のケース (コミット後に削除した BLOB の ID が再起動後に再利用される) の対応でもある。
      走査しなくなる現行世代のコンパクション済み WAL ファイルの分は、コンパクションカタログの max_blob_id が覆う
      (起動時は走査の値との max、コンパクションではカタログの更新が単調非減少。datastore.cpp:441、1276)。マージでは集めない。
* [x] コンパクションの入力から現行世代のコンパクション済み WAL ファイルを走査の入力から外し、最後のマージに渡す
      (datastore::compact_with_online()、dblogutil の compaction (dblogutil.cpp:434))。
      (compaction_options::set_compacted_input_file_name()。5 節。)
* [x] スレッド数: compaction_options の num_worker は、tsurugidb では recover_max_parallelism、dblogutil では --thread_num
      のままとし、意味をソートのスレッド数にする (L3 12.5・15)。dblogutil の compaction の --thread_num の既定値を 1 から
      8 にする (inspect と repair は走査のスレッド数のままで既定 1)。0 以下は設定の誤りとし、tsurugidb と tgreplica は
      起動を失敗させ、dblogutil は処理を始めずに終了する。(5 節。)
* [ ] 設定項目の受け取り (L3 12.5)。tsurugidb: tsurugi.ini の datastore 節の sort_arena_size_mb、sort_work_dir、
      sort_merge_fanin を tateyama から受け取るため、configuration に setter を加える (tateyama 側はステップ 14)。
      dblogutil: オプション --sort_arena_size_mb、--sort_work_dir、--sort_merge_fanin を加える。
      tgreplica: 環境変数 LIMESTONE_SORT_THREADS、LIMESTONE_SORT_ARENA_SIZE_MB、LIMESTONE_SORT_WORK_DIR、
      LIMESTONE_SORT_MERGE_FANIN で受け取る。tsurugidb はこれらの環境変数を参照しない。既定値は dblogutil の
      --thread_num を除き実行ファイルによらず同じ。
* [ ] 既存の compaction 配下、snapshot 配下 (snapshot_impl_test、cursor_impl_test)、log/dblog_scan_test が変えずに通ること。

### ステップ 7: 旧ソートの削除 (S1 メモ 5・6・8 節)

* [ ] sortdb_wrapper、旧ソート専用の関数 (comp_twisted_key()、insert_entry_or_update_to_max()、insert_twisted_entry()、
      split_db_value_and_blob_ids()、create_value_from_db_key_and_value() など) を削除する。
      (datastore_snapshot.cpp の旧ソート専用の関数は、使われなくなったステップ 6 のコンパクションの置き換えで削除した。
      残りは sortdb_wrapper と sorting_context。test/limestone/blob/datastore_blob_test.cpp:621 のコメントが
      insert_entry_or_update_to_max() に触れているので見直す。)
* [ ] CMake: RECOVERY_SORTER_KVSLIB・RECOVERY_SORTER_PUT_ONLY、マクロ SORT_METHOD_USE_ROCKSDB・SORT_METHOD_PUT_ONLY、
      cmake/FindRocksDB.cmake と RocksDB・LevelDB の find_package を削除する。
* [ ] .github/workflows/ci-build.yml:46 の -DRECOVERY_SORTER_KVSLIB=ROCKSDB と -DRECOVERY_SORTER_PUT_ONLY=ON を削除する。
* [ ] README.md の librocksdb-dev・libleveldb-dev の導入と注記 (19〜21 行)、ビルドオプションの説明 (47〜48 行) を削除する。
* [ ] テストの修正: snapshot/sorting_context_test (sortdb の部分を削除)、log/log_entry_4_LevelDB_test (write_serialized に
      置き換え)、compaction/compaction_boundary_scan_test (sortdb に触れているコメント)。
* [ ] RocksDB をインストールしていない環境でビルドとフルスイートが通ること。

### ステップ 8: 旧バージョンとの出力の比較 (S1 メモ 6 節、L3 8.7)

* [ ] 入力のデータを作る: 電話料金ベンチマークでデータを作り、料金計算のバッチとオンラインの更新を実行する。更新、削除、
      同じキーの複数の版を含める。
* [ ] 同じ入力のログディレクトリの写しから、旧バージョンと新バージョンのそれぞれで作る。
  * [ ] スナップショットファイル: tsurugidb を起動して停止する。
  * [ ] コンパクション済み WAL ファイル: tglogutil の compaction を実行する。
* [ ] 新旧のスナップショットファイル同士、コンパクション済み WAL ファイル同士が同一の内容であることを確かめる。
* [ ] 手順と結果を 5 節に記録する。

### ステップ 9: 測定と既定値の見直し (L3 13)

* [ ] スレッド数 (recover_max_parallelism) とアリーナの大きさ (sort_arena_size_mb) を変えて、起動時のソートの所要時間を測る。測定の条件は
      S1 メモ 3 節と同じにし、旧バージョンの結果 (ソート 18.2 秒) と比べる。
* [ ] 結果と考察をもとに、性能、安定性、使用するリソースが過大でないかを基準に既定値を決める。結果を 5 節に記録する。
* [ ] ソートの比較で呼ぶ関数 (参照型のアクセサ、key_sid の比較関数) を .cpp に置いたことの影響を確かめる。プロファイルで
      呼び出しのコストが目立つ場合に限り、根拠を添えて .h に戻すことを提案する (5 節)。
* [ ] 所有型の move とコピーのたびに参照型を作り直す (解析し直す) ことの費用を確かめる。目立つ場合は、バッファの置き場所が
      変わらない move では作り直さない形を検討する。
* [ ] write_serialized の、最も古い版に置き換えて書く指定 (fwrite を 3 回に分けて書く) の費用を、コンパクションの出力の
      時間で確かめる。目立つ場合は書き方を見直す。
* [ ] wal_sorter の add_entry で normal_with_blob 1 件ごとに blob_ids() が vector を確保することと、ワーカーごとの
      BLOB ID の最大値 (max_blob_ids_) が隣接していて false sharing になりうることの費用を確かめる。目立つ場合に限り、
      serialized_blob_ids の直接走査や alignas を検討する (ステップ 5 のレビューの指摘)。

### ステップ 10: LE 段階 2

* [ ] 分類の判定関数と、利用箇所の entry_type の列挙の置き換え (LE 9.1)。判定関数はステップ 2 で形式の定義のヘッダに
      置いた (5 節)。残りは利用箇所の置き換え。
* [ ] レプリカの受信で未定義の種別と現れてはならない種別を abort にする (LE 9.1、P21)。
* [ ] アクセサの種別の検査 (LE 9.2)。参照型と所有型 (旧アクセサの委譲) にはステップ 2 で入れた (5 節)。残りは、検査に
      合わせた利用箇所の確認と、旧名のアクセサの利用箇所を新しい名前へ移すときの見直し。
* [ ] 新しい名前のアクセサ (serialized_key_sid() など) の追加 (LE 10.3)。旧名は段階 4 まで残す。
* [ ] 型付き版の書き出しの長さの検査 (LE 12)。
  * [ ] 型付き版の書き出しは、1 エントリを書くのに fwrite・fputc を 7 回前後呼ぶ。ボトルネックになっているという報告は
        ないが、良い書き方ではない。長さの検査を加えるときに、見直すかを判断する。
* [ ] BLOB の GC の型 blob_gc_entry と、log_entry_container・log_entry_comparator の置き換え、log_entry_container.h の
      順序の文書の修正 (LE 13.1、P19)。
* [ ] UT: LE 15 節の abort する条件、書き出しの観点。

### ステップ 11: LE 段階 3

* [ ] 残りの呼び出し元 (cursor、compaction_carry_writer、レプリカの受信、テスト) を参照型と新しいアクセサに移す。
* [ ] std::istream から読む残りの呼び出し元を新しい読み込みに移す。
* [ ] cursor の move 後の状態の文書 (P20)。

### ステップ 12: LE 段階 4

* [ ] 旧 API を削除する: バイト列版の書き出し、旧名のアクセサ、truncate_value_from_normal_entry、
      static 関数 write_version_epoch_number・write_version_minor_write_version・parse_blob_ids、read と
      read_entry_from、所有型の write(FILE*)。make_normal_with_blob_log_entry はステップ 2 で前倒しして削除した (5 節)。
* [ ] 公開 API write_version_type(const std::string&)・write_version_type(std::string_view) の扱い (Q4 による)。

### ステップ 13: ディレクトリの fsync と BLOB の移動の fsync

* [ ] R1 で行うかを、ステップ 12 の終了時点の進捗状況で判断する。行わない場合は 6 節に記録して持ち越す。
* [ ] 名前の永続化を ext4・XFS の性質に任せるか、POSIX どおり親ディレクトリを fsync するかを決める (DF 10 節)。
* [ ] BLOB の移動による登録 (register_file の is_temporary_file が true) で、rename の後に移動先のファイルを fsync する
      (DF 9 節)。名前の永続化の判断によらず行う。公開 API の doxygen に、戻った時点で登録したデータが永続化されている
      ことを書く。
* [ ] 以下は、名前の永続化を POSIX どおりにすると決めた場合に行う。
  * [ ] ディレクトリを fsync する関数を file_operations に加える (テストで失敗を注入できるようにする)。
  * [ ] DF 6 節の箇所に、DF 8 節の指針のタイミングでディレクトリの fsync を入れる。pwal の新規作成とローテーションの
        rename、epoch ファイル、コンパクションカタログ、マニフェスト、コンパクション済み WAL とキャリーファイル、BLOB
        (作成、rename、hard link、サブディレクトリの mkdir)。
  * [ ] レプリカで BLOB を受信して書く経路の現状を確かめ、必要なら同じ対応をする (DF 10 節)。
  * [ ] スナップショットファイルと sorting の下の作業ファイルは対象外とする (DF 6 節)。
* [ ] UT: fsync の失敗の注入で例外になること。
* [ ] 行った場合は、L3 (全体) の 12.2 節と 17 節の申し送り事項を更新する (ステップ 14 の limestone 以外の対応)。

### ステップ 14: 仕上げ

* [ ] 既存の log_entry 関連のファイル (log_entry.h など) を src/limestone/log_entry/ へ移すかを決め、移す場合は移す。
* [ ] フルスイート、clang-tidy の警告の確認。
* [ ] コメントの英訳。
* [ ] PR の作成 (Q3)。
* [ ] limestone 以外のリポジトリの対応 (S1 メモ 8 節)。新しい limestone が取り込まれるまで、依存パッケージを外す変更は
      入れられない。ステップ 13 を行った場合は、L3 (全体) の申し送り事項の更新も含める。
  * [ ] tateyama: datastore resource の bridge (tateyama/datastore/resource/bridge.cpp) で、tsurugi.ini の datastore 節の
        sort_arena_size_mb、sort_work_dir、sort_merge_fanin を configuration に渡す。設定パラメータの文書
        (docs/config_parameters.md、docs/config-parameters-en.md) に新設の項目と recover_max_parallelism の意味の変更
        (ソートのスレッド数。メモリの使用量がスレッド数 × アリーナの大きさに比例する) を書く (L3 12.5・15・17 節)。
* [ ] master へのマージ前に本ファイルを削除する。

## 5. 記録

<!-- 判断した事項、計画からの変更、測定の結果を日付つきで追記する。 -->

* 2026-10-21: ディレクトリの fsync (L3 (全体) 17 節の申し送り事項) を検討し、DF にまとめた。BLOB の移動による登録では
  データの fsync を誰も行っていないことが分かり、limestone が行うと決めた (DF 9 節)。これらをステップ 13 として計画に
  加えた。R1 で行うかは進捗状況で判断する (ステップ 13 の先頭の項目)。S1 メモ 5 節の「fsync、close、キャリーファイルの作成の構造は変えない」
  から外れる変更であり、行う場合は計画からの変更となる。
* 2026-10-21: Q5 を決めた。段階 1 で旧アクセサの戻り値を string_view に変え、名前は段階 4 まで旧名のまま残す。LE 16.2 の
  段階のうち、10.3 節の戻り値の型の変更だけを段階 2 から段階 1 へ前倒しする。所有型の構成の変更を段階 2〜3 へ回す案は、
  所有型の生成と参照型への委譲を 3 本の std::string の構成で一度作ってから作り直す手戻りがあり、また 3 本の構成では
  所有型からエントリ全体を指す参照型を作れないため採らなかった。壊れる呼び出し元は、実装 1 か所、旧ソート数か所、
  テスト 1 か所である。
* 2026-10-21: 基準のビルドとフルスイート (ステップ 0)。コードは master 96fa064 と同じ (ブランチの差分は本ファイルだけ)。
  ビルドディレクトリ build (Debug、ENABLE_SANITIZER=ON、ENABLE_COVERAGE=ON、ENABLE_RDMA=OFF、RocksDB・PUT_ONLY)、
  gcc 13.3.0。ビルドは成功 (既にビルド済みで再コンパイルなし)。`ctest -j 16` で 103 件すべて成功 (11.7 秒)。
* 2026-10-21: ファイルの置き場所を決めた。新設する log_entry 関連のファイルは src/limestone/log_entry/ に置き、
  `#include "log_entry/..."` で参照する。新しいソートの部品 (ステップ 4) は別のディレクトリ (src/limestone/sort/ など) に
  置く。ソート済みファイルの形式 (LE 14) は log_entry/ に置く。既存の log_entry.h などは今は動かさず、後で移す
  (ステップ 14)。テストは test/limestone/log/ に置く。
* 2026-10-21: ステップ 2 の UT は、最後にまとめず項目ごとに書く (項目ごとに差分をレビューするため)。
* 2026-10-21: LE 10.2 からの変更。バイト列の読み書き (リトルエンディアンの整数の load・store) とフィールドの取り出しは、
  長さの確認を呼び出し側に委ねるので、形式の定義のヘッダ (log_entry/log_entry_format.h) で公開しない。参照型と所有型
  (ファクトリ、書き出しを含む) の実装の .cpp の無名名前空間 (または private static メンバ) に閉じ込め、外には長さが
  保証された操作だけを公開する。参照型は解析関数かファクトリでしか作れないようにし、「参照型があれば領域は形式どおりに
  揃っている」を型の不変条件とする。friend は使わない。形式の定義のヘッダには、形式の記述、長さの定数、key_sid の比較関数、static_assert だけを置く。(その後、entry_type の
  列挙、分類の判定関数、oldest_write_version() もこのヘッダに置いた。以下の記録を参照)
* 2026-10-21: LE 16.2 からの前倒し (ステップ 2 の参照型)。分類の判定関数 (LE 9.1) を形式の定義のヘッダに置き、参照型の
  アクセサには種別の検査 (LE 9.2。無効な値の要求で FATAL のログを書いて abort) を最初から入れる。参照型が安全の境界に
  なるため。所有型と既存の利用箇所への適用は LE どおり段階 2 (ステップ 10) で行う。参照型のアクセサは新しい型なので、
  LE 10.3 の新しい名前 (serialized_key_sid() など) を最初から使う。
* 2026-10-21: 分類の判定関数のために、entry_type の列挙を log_entry から形式の定義のヘッダ
  (log_entry_format::entry_type) へ移し、log_entry::entry_type はその別名とした。既存の利用箇所は変えずに通る。
* 2026-10-21: 所有型は、自分のバッファを log_entry_view::parse() にかけて参照型を得る (friend を使わないため)。
* 2026-10-21: 解析の結果 log_entry_parse_result はクラスにし、メンバ変数を private にした。結果は ok()、short_entry()、
  invalid_type() でだけ作り、読み出しは結果の種類で無効な値なら abort する。成功の結果の消費バイト数は参照型の長さから
  求め、両者が食い違う結果を作れないようにした。中身が状態で変わる型はクラス、ただの入れ物は struct という既存の
  使い分け (replication の validation_result など) に合わせた。
* 2026-10-21: 関数の定義は、強い理由がない限り .h ではなく .cpp に置く (探しやすさ、ビルド時間、カバレッジの確認で
  .h を見落とさないため)。参照型のアクセサと key_sid の比較関数も .cpp に置いた。LTO を使っていないので別の .cpp からの
  呼び出しは展開されない。inline 化が効くかはステップ 9 の測定で確かめ、効く関数だけを根拠を添えて .h に戻す。

* 2026-10-21: 所有型の構成 (ステップ 2 の 3 項目目)。所有型はバッファ (std::string) と、それを指す参照型をメンバに
  持つ。アクセサのたびに解析し直すと、ヘッダから長さを読み直す費用が呼び出しごとにかかるため。参照型を作るのは構築時と
  代入時だけとし、既存のオブジェクトを書き換える read_entry_from と truncate_value_from_normal_entry も、内部では新しい
  オブジェクトを作って move 代入する。定義は新設の src/limestone/log_entry.cpp に置いた。
* 2026-10-21: 旧アクセサは種別の検査ごと参照型に委ねた (LE 9.2 の所有型への適用を段階 2 から前倒し)。normal_entry に
  対する cursor の blob_ids() も abort するが、cursor の blob_ids() は公開 API ではなく、src と tsurugidb の他の
  コンポーネントに normal_entry で呼ぶ箇所はない。値のない欄を求めていたテスト (partitioned_cursor_impl_test の
  blob_ids()、AssertLogEntry に "" を渡していた 10 か所、log_entry_blob_test の marker の value_etc()) は、その欄を
  確かめない形に直した。
* 2026-10-21: 読み込みに失敗したとき、所有型は既定構築の状態になる (P5 の途中まで上書きされた状態をなくした)。
  short_entry でも read_error の種別を入れ、parse_wal_file の short_entry の振り分けを所有型の種別から
  read_error の種別に変えた (LE 11.3 のうち、この振り分けだけを段階 1 へ前倒し)。終端の判定は従来どおり eof で行う。
* 2026-10-21: 所有型の write(FILE*) は、持っているバイト列をそのまま書く。marker_end の CRC の種別は、no_crc に
  書き直さず読んだ値のまま書く。
* 2026-10-21: make_normal_with_blob_log_entry とそのテスト 2 件を、ステップ 12 から前倒しして削除した。src からの
  呼び出しはなく、1 本のバッファの構成で作り直す価値がないため。テストが確かめていた内容は、所有型の UT
  (log_entry_test.cpp) で確かめる。
* 2026-10-21: テスト用のバイト列を組み立てる関数群を、log_entry_view_test.cpp から共有ヘッダ
  test/limestone/log/log_entry_bytes.h へ移した。

* 2026-10-21: 最も古い版の write_version (LE 13.2) は、公開ヘッダの write_version_type ではなく、形式の定義のヘッダ
  (log_entry/log_entry_format.h) の関数 oldest_write_version() とした。コンパクション済み WAL ファイルの版は limestone の
  内部の約束事で、公開 API を増やす価値がないため。write_version_type のコンストラクタは constexpr でないので、定数に
  すると起動時の動的な初期化になる。それを避けるため、LE の「定数」ではなく値を返す関数にした。

* 2026-10-21: ファクトリ (LE 13.3) は、所有型の static 関数として種別ごとに 10 個設けた (make_normal_entry など)。種別を
  引数に取らないので、誤った種別を渡せない。長さの違反の UT は、key と value について、小さな領域を指し長さだけが
  大きい string_view で書いた (ファクトリはバイト列を読む前に abort する)。BLOB の個数の違反は、2^32 個の要素を
  作れないので UT を書かず、レビューで確かめる。

* 2026-10-21: write_serialized (LE 12) は、型付き版の書き出しと並べて所有型の static 関数にした (失敗時に errno 付きの
  例外を投げる write_bytes を使うため)。write_version の書き方は列挙型 write_version_handling (keep、oldest) の引数で
  指定する。コンパクションの出力は実行時に置き換えの有無を切り替える形のため。oldest は marker では参照型の
  serialized_value_etc() が abort する。oldest は、write_version の手前、(0,0)、後ろの 3 回の fwrite で書く。1 回で書く
  にはエントリ全体のコピーが要る。fwrite はユーザ空間のバッファへのコピーなので、呼ぶ回数が増えてもシステムコール
  は増えない、という推論による (測定はステップ 9)。

* 2026-10-21: ステップ 2 を完了した。master からの変更のうち src の追加・変更行について、行のカバレッジを lcov で確かめた。
  実行可能な 419 行のうち 386 行 (92%) が実行された。残りの 33 行は、死亡テストで確かめている FATAL (子プロセスの
  カバレッジは記録されない)、後のステップで消える旧ソート、到達しない防御のコード (rebuild_view の形式の違反、
  BLOB の個数の違反、dblogutil の default など)、コンパイラが生成した例外の経路である。to_string_view と operator<<
  (entry_type、log_entry_parse_result::status) は未実行だったので UT を追加した。

* 2026-10-22: ファイルからの読み込み (LE 11.2) を log_entry/log_entry_reader.h の log_entry_reader とした。結果は
  log_entry_read_result (ok、end、error の 3 種類。error は log_entry::read_error を持つ)。読み出しは read ではなく
  pread を使う。オフセットは読み込みが自分で数えるので lseek が要らず、ファイルの位置を共有しない。pread は POSIX.1-2008 の
  基本仕様にあり、limestone は既に POSIX を前提にしている (open、fsync、flock) ので、移植できる範囲は狭まらない。
* 2026-10-22: バッファの通常の大きさは 16 MiB の固定値とし、ファイルの大きさによらず常にこの大きさで確保する
  (std::string でゼロで埋める)。この環境で測ると、32 MiB をゼロで埋めて確保・解放する費用は 1 回 7.7 ms で、中身は
  ページフォールトだった (初期化しなくても全体に触れれば同じ)。差が出るのは通常の大きさより小さいファイルだけで、
  起動時の増加は (小さいファイルの数 × 1 回の費用 ÷ 走査のスレッド数) と見積もった。ファイルが 1,000 になることは
  現実的にないので許容し、確保の大きさをファイルの大きさに合わせる分岐と、初期化しない確保は採らない (保守性と、
  未初期化の領域を読む誤りを ASan で検出できなくなるため)。epoch ファイルは毎秒 100 回程度追記されることがあり、
  小さいとは限らない。
* 2026-10-22: テストでの差し込みのために、file_operations に pread と fstat を加えた。読み込みはコンストラクタで
  file_operations を受け取る (ファイルを開くのが構築時のため、setter にはしない)。file_operations を受け取る
  コンストラクタはテスト専用で、本番のコードは real_file_operations を使うコンストラクタだけを使う。
  open に O_CLOEXEC は付けない。既存のコード (manifest.cpp など) は付けておらず、付けたコードと付けないコードが
  混在すると、付ける意図が分からず保守の妨げになるため。limestone には子プロセスを起動するコードがない。補充の 3 分岐は、通常の大きさを
  超えるエントリと境界をまたぐエントリを実際に作り、pread の要求の大きさで確かめる。errno を付けない
  limestone_io_exception を投げるマクロ LOG_AND_THROW_IO_EXCEPTION_NO_ERRNO を、BLOB の同名のマクロに倣って加えた。

* 2026-10-22: 書き込み中のファイルの読み出し (LE 11.5) を log_entry_reader に加えた。状態は file_write_state
  (writing、written、failed)、状態の関数は file_write_state_function (std::function)。コンストラクタは、本番用が
  (file, start) と (file, start, state) の 2 つ、テスト用が (file, start, state, file_ops) の 1 つ (state が空なら
  書き込み中でないファイル)。待つ間隔は 10 ms の固定値 (wait_interval) で、テストでも差し替えない。状態の関数を渡した
  ときは、LE 11.5 のとおり、ファイルの不在と、読み始めの位置がファイルの大きさを超えることを例外にせず待つ (渡さない
  ときは従来どおりコンストラクタで例外。引数で振る舞いが分かれることは了承済み)。
* 2026-10-22: 状態の関数を先に呼び、その後でファイルの大きさを取る。逆の順序だと、その間に完了した書き込みの分を
  取りこぼし、末尾で不足と誤って判定しうるため。読み込みは独自のタイムアウトを持たず、状態が書き込み中である限り
  待つ。打ち切りは、転送の I/F の I/O タイムアウトにより状態の関数が failed を返すことで行う (L3 12.2)。
* 2026-10-22: 書き込み中のファイルの UT は、書き込む側のスレッドを使わず、呼ばれるたびにファイルへ追記してから状態を
  返すテスト用の状態の関数で書いた。状態の関数は読み込みが待つ地点で呼ばれるので、「待つ間に続きが届く」状況を
  決定的に再現できる。実際のファイル転送と組み合わせた確認は、シナリオテストとして持ち越す (6 節)。

* 2026-10-22: parse_wal_file と dblog_scan (last_durable_epoch) の読み込みを log_entry_reader に移した。pWAL ファイルは
  読み取り専用で開き、無効なスニペットの marker の書き換えは、書き換えるたびに別に書き込み用に開いたファイルへの
  pwrite と fsync で行い、閉じる (::open・::pwrite・::fsync を直接使い、file_operations は通さない)。add_entry には、参照型から作った所有型を
  渡す (参照型のまま渡すのはステップ 5)。
* 2026-10-22: marker の書き換えに失敗したら (書き込み用に開けない、pwrite が失敗する)、limestone_io_exception を投げる。
  当初は旧実装に合わせて LOG(ERROR) で続行する案としたが、旧実装は走査の最初に読み書き両用で開いていたため、書き込み
  権限がなければ開く時点で例外になっていた (dblogutil_test の repair_cannot_modify)。読み取り専用で開くように変えると、
  書き換えられないまま修復済みと報告することになる。書き換えられないまま続行すると、durable epoch が進んだ後の起動で、
  無効にすべきスニペットが有効なデータとして扱われうる。修復の設定なら走査の最初に書き込み用に開く案もあったが、
  場当たり的なので採らず、書き換えそのものの失敗を例外にした。失敗する時点は、走査の最初から最初の書き換えの時点に
  移るが、どの呼び出し元でも例外で全体が失敗するので、結果の正しさには影響しない。開けた後の pwrite の失敗も例外にした。旧実装は
  LOG(ERROR) を出して続行し、修復済み・正常終了と報告していた (書き換えに失敗しているのに正常終了するバグ)。
  書き換えの後に fsync を行う (旧実装は TODO のまま行っていなかった)。書き換えはまれなので費用は無視できる。fsync の
  失敗も例外にする。
* 2026-10-22: 読み込みの error の後の振る舞いを考え直した。項目 1 で決めた「end または error の後は同じ結果を返す」は
  要件から導いたものではなく、実装の都合で決めたものだった。parse_wal_file の DFA は、切れた marker_invalidated_begin
  (SHORT_marker_inv_begin) を無視して次を読み、終端で抜ける。旧実装は short_entry の後に終端を返していたが、新しい
  読み込みが同じ short_entry を返し続けたため、無限ループになった。要件 (ファイルの実態) から、short_entry の後は
  ファイルの大きさの位置で end を返すことにした (切れたエントリはファイルの末尾にあり、後ろにバイトはない。下限が
  ファイルの残りを超えたときは、まだ読んでいないバイトも飛ばす)。unknown_type の後は同じ unknown_type を返す
  (エントリの境界を区切り直す方法がない)。unknown_type の後の扱いは確信が持てないが、確信を得る条件も思いつかない
  ので、このまま進め、問題が出たら考え直す。

* 2026-10-22: LE 15 節の観点と log_entry_reader_test の対応。ファイルからの読み込み: 補充の 3 分岐は
  buffer_expands_and_shrinks (拡大・縮小)、buffer_moves_unparsed_to_front (先頭への移動)、buffer_expands_from_middle、
  buffer_expands_twice_for_blob_ids。read が要求より少ない場合は short_reads、short_reads_after_expansion。終端と
  short_entry の区別は reads_entries_in_order、empty_file_is_end、truncated_entry_is_short_entry、
  truncated_large_entry_does_not_expand。長さフィールドが残りを超えても確保しないことは length_beyond_file_does_not_allocate、
  truncated_large_entry_does_not_expand、short_entry_skips_unread_bytes。指定した位置からの読み始めは starts_at_offset、
  start_offset_beyond_file_size_throws。書き込み中のファイル: 書き込み済みへの遷移は writing_waits_until_written、
  writing_waits_for_file_to_appear、writing_waits_for_start_offset。失敗への遷移は writing_failed_throws、failed_is_final。
  途中で待って解析が成功することは writing_waits_until_written。書き込み済み後の終端と不足の区別は
  writing_waits_until_written、written_then_short_entry、written_while_waiting_then_short_entry。下限がバッファを超える間
  に確保しないことは writing_does_not_allocate_while_waiting、written_while_waiting_then_short_entry。

* 2026-10-22: ステップ 3 全体 (4975b83..25c5852) に /crv (Sonnet・Opus・Fable) と /cr を実施した。対応したもの:
  update_write_state() が writing_ = false を先に立ててから例外を投げ、例外の後の next() が黙って end を返していた
  (検査がすべて通った後に切り替える形に直し、UT で例外が続くことを確かめた)。clang-tidy の警告 9 件 (pointer-arithmetic、
  implicit-widening、pass-by-value)。marker の書き換え失敗の直接の UT (読み取り専用のファイルで EACCES)。doxygen の
  @throws のずれ。待つたびに begin_ == 0 でも memmove していた無駄。last_durable_epoch の例外メッセージにパスと位置を
  加えた。細部 (errno の取り方の統一、{} 初期化、<cerrno>)。pwrite の前の errno = 0。ディレクトリを読ませて read のエラーを
  起こす dblog_scan のテストは、st_size と EISDIR に依存して脆いので削除した (read のエラーの区別は読み込み側の差し込みの
  テストで確かめている)。見送り: なし。
* 2026-10-22: ステップ 3 の src の変更行のカバレッジ (レビュー対応前の 25c5852 時点)。実行可能な 234 行のうち 222 行 (94%) が
  実行された。未実行は、死亡テストの FATAL 4 行、marker の書き換えの pwrite・fsync の失敗 6 行 (差し込めない)、
  有効なスニペットの中の切れたエントリ・不正な種別に対する repair_by_mark の invalidate の呼び出し 2 行 (既存のテストが
  通していなかった経路で、引数を変えただけ)。
* 2026-10-22: CI (ci-build.yml) の CTest のステップを一般ユーザ (useradd した tester) で実行するようにし、master 4dce266 に
  入れた。root ではファイルの権限が効かず、権限に関するテストがスキップされていたため。CI で 4 件のテストが実行されるように
  なったことを確かめた。

* 2026-10-22: ステップ 3 全体に 2 回目の /crv (Sonnet・Opus・Fable) と /cr を実施した。3 モデルとも 1 回目の対応は正しいと
  確認。対応したもの: 読み取り専用の pWAL を repair_by_cut で切り詰めると boost::filesystem::resize_file の例外が走査の
  ワーカースレッドの catch (limestone_exception) で受けられず std::terminate していた (読み取り専用で開くようにしたことで
  通るようになった退行。例外を投げない版の resize_file で limestone_io_exception に変換し、UT を加えた)。private 関数の
  @throws の不足、dblog_scan_test の <unistd.h>、invalidate_epoch_snippet の @brief (理由のコメントは実装側に移した)。
  見送り: 待つループ側で failed になる UT (共通の関数で新しい経路を通さない)、契約外のファイルの縮小での end の位置、
  テストの入れ物の struct のメンバの _ 接尾辞 (既存の入れ物の struct も付けていない)。

* 2026-10-26: L3 のレビュー対応 (c710f4d → 7de8a93) で変わった仕様を計画に反映した。
  * 設定項目の指定の手段 (12.5 節): 環境変数だけだったものを、tsurugidb は tsurugi.ini の datastore 節、dblogutil は
    コマンドラインオプション、tgreplica は環境変数にした。スレッド数は既存の recover_max_parallelism と --thread_num の
    意味を変えて使い、LIMESTONE_SORT_THREADS は tgreplica だけが読む。新設は sort_arena_size_mb、sort_work_dir、
    sort_merge_fanin (dblogutil は同名のオプション、tgreplica は LIMESTONE_SORT_ の環境変数)。スレッド数 0 以下は設定の
    誤り。dblogutil の compaction の --thread_num の既定値は 1 から 8 に変わる (15 節)。ステップ 4 の環境変数の項目を
    「部品は値で受け取る」に変え、指定の手段をステップ 6 に、tateyama 側の対応をステップ 14 に置いた。tsurugi.ini の項目は
    tateyama が configuration で渡すので、configuration に setter を加える (公開 API の追加)。
  * マージの段数 (8.6 節): 1 段の条件を「2 以上かつマージの上限以下」とし、ソート済みファイルが 0 個・1 個のときも最後の
    マージと同じ処理で出力を作ることが明記された (ソート済みファイルは形式が出力と違い、範囲削除の適用も最後のマージで
    行うため)。ステップ 4 のマージと UT の項目に加えた。
  * ソートの出力 (8.1 節): 適用箇所ごとの出力 (起動時はスナップショットファイル、コンパクションは新しい世代の
    コンパクション済み WAL ファイル) が明記され、ソート済みファイルは中間のファイルで出力ではないとされた。起動時に作った
    スナップショットファイルを次回以降の起動に使うことは M1 では行わない。計画の変更はない。
  * R1 に影響しない変更: マーカーファイル remote_recovery_in_progress (6.1・6.8 節)、ロケータファイルのヘッダと終端
    (9.1 節、S2)、tglogsender のマニフェストの flock (12.4 節)、空き容量の確認と BLOB の一覧ファイル (6.5・6.6 節)、
    コンパクション済み WAL ファイルの分割と tgreplica の systemd への通知の申し送り (17 節)。
  * S1 メモ 4 節は環境変数の名前のままで更新されていない。既定値 (スレッド数 8、アリーナ 256 MiB) はそのまま使う。

* 2026-10-26: ステップ 4 のバッファ (sort_buffer) の設計。
  * インデックスの要素はアリーナ内の位置だけ (std::uint32_t。アリーナの上限は 4 GiB で、超える指定は FATAL。設定値の検査は
    ステップ 6)。key_sid の位置や write_version の写しを要素に持つ案は、
    key_sid が可変長で面倒なうえ、比較は key_sid のバイト列をアリーナから読むので種別と key の長さを同時に読む費用は
    小さいと見込み、採らない。性能はステップ 9 の測定で確かめ、遅ければ見直す (要素の中身は sort_buffer の内部)。
  * アリーナを超えるエントリの判定は append() の中で行い、結果を 3 値 (appended、full、oversized) で返す。呼び出し側が
    事前に大きさを比べる形は、「満杯なら書き出して入れ直す」ループに超過のエントリが紛れ込む罠があり、FATAL の検査が
    要るので採らない。単独のソート済みファイルへの書き出しは呼び出し側 (ステップ 5) の仕事。
  * 置き場所は src/limestone/sort/ と test/limestone/sort/ (両方の CMakeLists.txt の GLOB に追加)。
  * アリーナは std::string でゼロ初期化し、最初の append() で確保、clear() では解放せず再利用する。大きさはバイトで
    受け取り、0 は FATAL。append() はデータ系以外で FATAL。entry_at() は位置から parse() した参照型を返す。コピーと
    ムーブは禁止 (スレッドごとに 1 つ、その場で構築する)。
  * 走査とアリーナへのコピーは同じスレッドで行う (dblog_scan のワーカーが add_entry の中で自分の sort_buffer に
    append() する。L3 8.4、S1 メモ 5 節)。スレッドを分けて渡す形ではない。

* 2026-10-26: ステップ 4 のソート (sort_buffer::sort())。
  * 比較で使う読み出し (load_uint32le、load_uint64le) を log_entry_view.cpp の無名名前空間から log_entry_format へ移し、
    鍵の取り出し (data_entry_key_sid、data_entry_write_version: 種別と key の長さだけ読み、parse() は呼ばない) と
    key_sid_offset(entry_type) を加えた。log_entry_view::key_sid_offset() は format のものに委ねる (LE 10.2 の一元化)。
  * 縮約はバッファでは行わず、書き出し側で「直前に出した key_sid と同じなら書かない」として行う。バッファで std::unique
    により縮約する案は、ソート後のインデックス順にアリーナをもう 1 回ランダムに読む走査 (エントリごとにキャッシュミス)
    が増えるので採らない。書き出し側なら、書き出しのために読むエントリを直前のエントリと比べるだけで済み、アリーナの
    走査は 1 回で終わる。マージの出力の縮約 (L3 8.6「どのマージでも縮約する」) も同じ規則なので、実装を共有する。
  * 同じ key_sid かつ同じ write_version は内容が同一 (L3 8.2) なので、std::sort が不安定でも残る 1 件は問わない。
  * write_version の比較は write_version_type の operator< (epoch、minor の数値) で行う。

* 2026-10-26: ステップ 4 のソート済みファイルの書き出しと読み込み、縮約。
  * 縮約は key_sid_deduplicator (accept() が「直前に通した key_sid と違えば true」) に切り出した。直前の key_sid は写しを
    持つ (入力の参照型は次の読み込みで無効になるため)。比較で順序違反 (直前より小さい) が分かるので、それは FATAL とし、
    呼び出し側の「ソート順で渡す」契約を構造で検査する。最後のマージの出力にも同じものを使う。
  * sorted_file_writer は write(view) と close() (fflush、fclose。fsync は下の 2026-10-27 の記録でやめた) を持ち、書き出しは既存の
    log_entry::write_serialized(keep) を使う。stdio のバッファは writer が自前で持つ 128 KiB (glibc の setvbuf は
    バッファに NULL を渡すと大きさを無視するので、既存の snapshot の書き出しの setvbuf(nullptr, _IOFBF, 128 KiB) は
    効いていない。大きさの効果はステップ 9 で測る)。close() の前に破棄されたときは fclose だけ行う (例外で抜けるときの後始末)。fwrite/fsync の失敗は注入で
    起こす手段がない (write_serialized が FILE* を直接取る) ので UT にはない。
  * (sorted_file_reader はマージの項目で削除した。下の記録を参照。)
  * sort_buffer の書き出しは、呼び出し側が entry_at() の順に write() を回す (sort_buffer と writer を結ぶ関数は置かない)。

* 2026-10-26: ステップ 4 の置き場所 (sorting_directory)。
  * 構築で parent/sorting を作る (すでにあれば remove_all してから)。remove() が中身ごと削除して失敗を例外で伝え、
    remove() なしの破棄ではデストラクタが削除する (失敗は無視)。sorted_file_writer の close() とデストラクタと同じ形。
  * ソート済みファイルのパスは new_file_path() (sorting/sorted-<連番>、atomic の採番) で取る。ファイルは作らない。
  * 「1 つの limestone インスタンスが専有する」(L3 8.5) の検査は行わない (仕様に規定がない)。
  * 失敗系の UT は親ディレクトリの権限を落として起こす (CI のテストは非 root で動く)。

* 2026-10-26: ステップ 4 のマージ (sorted_file_merger)。
  * merge() が中間のマージと最後のマージをまとめて行う。中間のマージは入力の先頭から上限の個数ずつをまとめて
    sorted_file_writer で新しいソート済みファイルに書き (縮約は writer)、入力を削除する。最後のマージは
    key_sid_deduplicator → 範囲削除 → コールバック。範囲削除の検査は deletions が空なら行わない。
  * k-way マージは std::priority_queue (入力の番号を要素に、各入力の現在の参照型で比較)。取り出したエントリを呼び出し側が
    使い終えた後 (次の next() の先頭) でその入力を進める。出力 1 件あたりの比較は約 2 log2 k (pop と push)。ステップ 9 の
    測定で比較が目立てば、先頭の置き換え (自前のヒープで sift-down、約 log2 k) → トーナメント木の順に検討する。
  * コンパクション済み WAL ファイルは compacted_wal_input (パスと、normal_with_blob を読むたびに呼ぶコールバック) で
    最後のマージの入力に加える。BLOB の GC の記録の中身は呼び出し元 (ステップ 6) が決める。
  * sorted_file_reader (log_entry_reader を包んで形式の誤りを例外に変える層) は削除した。reader ごとに失敗の伝え方が
    違うのは脆いため。マージは log_entry_reader を直接使い、形式の誤り (error、データ系でないエントリ) を例外にする
    変換と、先頭の marker_begin の読み飛ばし (指定も検査もしない。marker_begin を将来なくすときの修正量を減らす) は、
    入力を進める 1 か所 (read_next_entry) に置いた。
  * マージの入力 1 本あたりの読み込みバッファは log_entry_reader の 16 MiB のまま (上限 128 で 2 GiB)。ソートのスレッドが
    終わってアリーナ (8 × 256 MiB) を解放した後に始まるのでピークは増えず、HDD では大きいほうがシークが減る。設定の文書
    (ステップ 14) にメモリの目安 (スレッド数 × アリーナ + 上限 × 16 MiB) を書く。大きさの効果はステップ 9 で測る。
  * マージの上限 2 未満は FATAL (設定値の検査はステップ 6)。
* 2026-10-27: ステップ 4 全体のレビュー (3 モデル、cr-sort-20261027.md に指摘と対応をまとめた) の判断。
  * ソート済みファイルの close() で fsync しない (fflush と fclose だけ)。一時ファイルで、同じプロセスが読み戻して
    削除するので、永続化は要らない。limestone はファイルシステムが write の内容を read で返すことを前提に設計して
    おり、一時ファイルにだけ fsync による書き出し失敗の検出を求めるのは一貫しない。writeback の失敗はまれで、
    起きても読み戻しの形式検査 (ゼロの領域は不正な entry type か途中で切れる) で例外になる。
  * 中間マージは「超過分 + 1」本 (上限との小さいほう) だけをまとめる。上限をわずかに超えたときに全データを書き直す
    無駄をなくす。
  * アリーナは値初期化しない確保 (unique_ptr<char[]>)。std::string の assign だと最初の append() で全ページを
    ゼロ埋めし、入力が小さくても全量をコミットするため。
  * 変えないもの: 入力の key_sid の順序違反は key_sid_deduplicator の FATAL のまま (コンパクション済み WAL
    ファイルも limestone が key 順に書いたもので、旧実装の順序 (bytewise) と compare_key_sid は一致する)。
    remove_directory (操作とエラー処理を持つ補助関数、2 か所から呼ぶ) はそのまま。同一 (key_sid, write_version) で
    内容が違うエントリのどれが残るかは不定 (入力番号で決めてもスレッドへの割り当てが不定なので決定的にならない。
    旧実装も同じ。正常な WAL では生じない)。
* 2026-10-27: ステップ 5 全体のレビュー (3 モデル、cr-step5-20261027.md に指摘と対応をまとめた) の判断。
  * 直したもの: scan() の末尾でワーカーを解放し、アリーナ (既定 8 × 256 MiB) を merge() の間まで持ち越さない。
    scan() が失敗した後の merge() は FATAL (成功時だけ立てるフラグで検査する。directory_ の有無では失敗した scan() を
    通してしまう)。
  * dblog_scan のワーカーが捕捉するのは limestone_exception だけ (構造は以前から)。他の例外はスレッドから漏れて
    terminate する。フックと add_entry の先 (sort_worker、sorted_file_writer、sorting_directory) が投げるのは
    limestone_io_exception だけと確認した。ステップ 6 で limestone が書く on_normal_with_blob のコールバックも
    limestone_exception に揃える。catch(...) は足さない。
  * テストでタイミングに依存してよいのは退行の検出率だけ。基準は「正しい実装なら結果がタイミングによらず一意に
    決まる」こと。input_file_queue_test の popper::still_waiting (100 ms 待って pop() が戻っていないことを見る) は、
    正しい実装では pop() が戻らないのでタイミングで失敗することがなく、この基準の範囲内として残す。sleep で待ち合わせて
    から次の操作をする形 (タイミングで結果が変わる) のテストは置かない。
  * merge_fanin はコンストラクタで検査しない (設定値の検査は入口で行う決定のとおり。sorted_file_merger の FATAL は
    契約違反の検出として残す)。

* 2026-10-28: ステップ 6 の進め方と出力の書き出し。
  * 進め方: 出力の書き出し → 起動時の置き換え → コンパクションの置き換え (入力は変えない) → 現行世代のコンパクション
    済み WAL ファイルを最後のマージへ移す → スレッド数の意味と検査 → 設定項目の受け取り、の順に 1 項目 1 コミット。
    BLOB ID の最大値を走査で記録する項目は、起動時とコンパクションの置き換えで実現する。
  * 走査の失敗を "dblogdir is corrupted" に変える変換では limestone_io_exception をそのまま通す。sorting への書き出しの
    I/O の失敗 (ディスク満杯など) を形式の誤りに変えると、オンラインのコンパクションが再試行でなく FATAL になるため。
  * 出力の書き出しは sorted_file_writer と共通化せず、別クラス sort_output_writer (先頭の marker_begin(0)、
    write_version の扱い、remove_entry を書くか、close で fsync するかを format で指定) にした。L3 で「ソート済み
    ファイル」は中間のファイルの名前で出力と区別されていること、最後のマージは縮約済みで writer の縮約が空回りすること
    による。共通の stdio の扱い (自前のバッファで開く、close と sync_and_close、close なしの破棄での fclose) は
    buffered_output_file に切り出し、sorted_file_writer もそれを使う形にした。一時ファイルと rename、失敗時の削除は
    呼び出し側の仕事のまま。
  * 多重クローズは WARNING で何もしない (実害がなく、tsurugidb を落とすのは過剰なため)。例外は fsync せずに閉じた後の
    sync_and_close() で、戻ると fsync されたと誤認させるので FATAL。閉じた後の stream()・write() は FATAL のまま。
    sorted_file_writer の 2 回目の close() も FATAL から WARNING に変わった。

* 2026-10-28: ステップ 6 の起動時の置き換え。
  * wal_sorter::scan() は、コンパクションの境界 (optional) と durable epoch を別の引数で受け取り、片方だけを使って
    いた。どちらも「これを超える epoch のスニペットをソートに含めない」境界で、違いは超えたスニペットの扱い
    (起動時とオフラインのコンパクションは invalidated に書き換え、オンラインのコンパクションはファイルを変えずに
    読み飛ばす) だけなので、境界の epoch と beyond_boundary_handling の 1 組にまとめた。引数の意味を明確にするため。
  * スレッド数は recover_max_parallelism (既定 8) がそのまま効く。旧ソートは RocksDB で PUT_ONLY でない構成のとき 1 に
    固定していた。
  * スナップショットの書き出しの失敗は、ファイルを閉じてから例外になる (旧コードは FILE* がリークしていた)。fsync は
    しない (起動ごとに作り直すため。旧コードと同じ)。

* 2026-10-28: ステップ 6 のコンパクションの置き換えのフルスイートで、1 回だけ replica_server_test が失敗した。
  * 失敗したケースは不明 (ログを上書きした。残っていた LastTestsFailed.log でテストの名前だけが分かった)。その後の
    ctest -j 16 の 16 回と -j 32 の 10 回、単独での 30 回の繰り返しでは再現しない。RUN_SERIAL の外から ctest と
    並行に繰り返すと、initialize_rdma_success_then_already_initialized で 1 回目の initialize_rdma() が failed を
    返すことはある (RDMA スタック (GnMock) の排他の前提を破った条件で、元の失敗と同じとは限らない)。
  * RDMA の初期化はソートの変更と関わらないので、関係は薄いと見て深追いしない。以後のフルスイートは
    --output-on-failure で失敗時のログを残し、再発したらそのログで調べる。

* 2026-10-28: ステップ 6 の現行世代のコンパクション済み WAL ファイルの扱い。
  * 呼び出し側 (オンライン、dblogutil) がカタログの現行のファイル名を compaction_options に渡し、options が走査の
    ファイルの集合から外して記録する。名前が集合になければ何もしない (その回の入力ではないため)。
  * make_dblog_scan() の「ディレクトリ全体を走査する」条件を、集合が空から「集合の指定がない (has_file_set() が
    false)」に変えた。dblogutil で入力が現行世代のファイルだけのとき、外すと集合が空になり、旧条件では外したファイルまで
    走査してしまうため。起動時に入力の集合が空のときは、ディレクトリ全体 (カタログが除外したファイルを含む) でなく
    何も走査しない動作に変わる。
  * BLOB の GC の記録はマージのスレッドからも呼ぶ。マージの後に finalize_local_entries() でそのスレッドの thread_local
    のコンテナを切り離す (dblog_scan のワーカーがファイルごとに行っているのと同じ決まり)。

* 2026-10-28: ステップ 6 のスレッド数の検査。
  * datastore のコンストラクタで recover_max_parallelism < 1 を initialization_failure にする (tsurugidb と tgreplica の
    両方が通る)。検査はログディレクトリのロックを取る前に置く。ロックの後で throw するとロックが解放されず、同じ
    プロセスで作り直せない (UT で判明)。
  * dblogutil は、compaction で --thread_num の指定がなければ sort_settings の既定値 (8) を使う (gflags の is_default で
    判定)。0 以下は 3 つのサブコマンドとも処理を始めずに終了コード 64。compaction は使うスレッド数を
    "sort-threads: N" として標準出力に出す (既定値の変更に利用者が気づく助けと、UT の確認の手段)。

## 6. 残った課題

<!-- R1 の範囲外として持ち越すものを記録する。 -->

* log_entry::read_error::operator bool() は、警告 (nondurable_snippet) でも true を返す (value_ != 0)。使われ方
  (読み込みの失敗の判定) からは false が適切。今のコードには警告を bool に変換する経路がないので、動作への影響は
  ない。変えるなら、警告とエラーの判定 (コードの値の最上位ビット) を含めて設計する。(2026-10-22)
* 書き込み中のファイルの読み出し (LE 11.5) を、実際のファイル転送 (L3 12 節の pull) と組み合わせて確かめる
  シナリオテスト。状態の関数の本体と転送のサービスを実装するときに、UT と同じ枠組み (gtest、CI で実行) で書く。
  R1 の UT は、テスト用の状態の関数で読み込みの論理だけを確かめる。状態を先に見てから大きさを取る順序の正しさなど、
  書き込みとの競合は UT では狙って通せない。(2026-10-22)
* 書き込みの状態の型 (file_write_state、file_write_state_function) は、作り手であるファイル転送の側に置くのが
  本来と考える。転送の実装がまだないので、R1 では log_entry/file_write_state.h に置いた。転送を実装するときに、転送の
  側へ移すかどうかを決め直す。(2026-10-22)
* dblog_scan::scan_pwal_files() は、走査 1 回分の共有状態をローカル変数に持ち、ファイル 1 つの処理とワーカーの本体を
  `[&]` のラムダで書いているため、どの処理がどの状態を使うかが読めない。走査の状態を構造体に、ラムダを名前付きの
  メンバ関数にする整理が要るが、R1 のスコープ外とした。(2026-10-27)
