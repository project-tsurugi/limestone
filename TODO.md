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
* [ ] 最も古い版の write_version の定数 (LE 13.2)。
* [ ] ファクトリ (LE 13.3)。データ系・ストレージ系・marker の各種別。長さの違反で abort。
* [ ] write_serialized と、最も古い版に置き換えて書く指定 (LE 12)。
* [ ] UT: LE 15 節の解析関数、比較関数、所有型、write_serialized の観点。

### ステップ 3: ファイルからの読み込み (LE 段階 1 の後半)

* [ ] POSIX の read による読み込み (LE 11.2)。バッファの補充の 3 分岐、位置を指定した読み始め、終端と short_entry の区別。
* [ ] 書き込み中のファイルの読み出し (LE 11.5)。状態の関数、待ち方。
* [ ] parse_wal_file と dblog_scan の読み込みを新しい読み込みに移す。無効なスニペットの marker の書き換えは、別に開いた
      ファイルへの位置を指定した書き込みに変える (LE 11.2)。失敗時の判定を read_error の種別に変える (LE 11.3)。
* [ ] UT: LE 15 節のファイルからの読み込み、書き込み中のファイルの読み出しの観点。
* [ ] 既存の dblog_scan・parse_wal_file・compaction のテストが変えずに通ること。

### ステップ 4: 新しいソートの部品 (L3 8.2〜8.6)

* [ ] バッファ: 固定長のアリーナとインデックスの配列。満杯の判定、アリーナを超えるエントリの単独書き出し (8.3)。
* [ ] インデックスに対する std::sort。比較はアリーナ内の key_sid と write_version を直接読む (8.2・8.3)。ソート後に縮約する。
* [ ] ソート済みファイルの書き出しと読み込み (LE 14)。末尾で不足と不正は limestone_io_exception。
* [ ] 置き場所 sorting の作成と削除。LIMESTONE_SORT_WORK_DIR、未指定はログディレクトリ (8.5)。
* [ ] マージ: マージの上限 (LIMESTONE_SORT_MERGE_FANIN、既定 128) による中間のマージと最後のマージ。どのマージでも縮約し、
      範囲削除の適用は最後のマージだけで行う。マージは 1 本のスレッド (8.6、S1 メモ 2 節)。
* [ ] 最後のマージの結果を参照型でコールバックに渡す (S1 メモ 2・5 節)。
* [ ] コンパクションの最後のマージの入力に現行世代のコンパクション済み WAL ファイルを加える。先頭の marker_begin を読み飛ばし、
      normal_with_blob を BLOB の GC のために記録する (8.6)。
* [ ] 環境変数 LIMESTONE_SORT_THREADS (既定 8)、LIMESTONE_SORT_ARENA_SIZE_MB (既定 256)、LIMESTONE_SORT_MERGE_FANIN
      (既定 128)、LIMESTONE_SORT_WORK_DIR (S1 メモ 4 節、L3 12.5)。
* [ ] テストからマージの上限とアリーナの大きさを小さく指定する手段。
* [ ] UT: 順序と縮約、範囲削除の適用、多段のマージ、アリーナを超えるエントリ、sorting の作成と削除 (S1 メモ 6 節)。

### ステップ 5: dblog_scan の拡張 (L3 8.4、S1 メモ 5 節)

* [ ] 入力のファイルを、閉じられるまで待つキューから取り出す形にする。起動時とコンパクションでは、すべてのパスを入れてから閉じる。
* [ ] キューの中断 (失敗時。スレッドは読みかけのファイルを打ち切って終了し、マージは行わない)。
* [ ] スレッドの終了時のフック (そのスレッドのバッファの残りをソートして書き出す)。
* [ ] dblog_scan のスレッドをソートのスレッドとし、add_entry が呼ばれたスレッドのアリーナに詰める。
* [ ] UT: キューを閉じたときと中断したときの動作、失敗の伝え方。

### ステップ 6: 呼び出し元の置き換え (S1 メモ 5 節)

* [ ] create_snapshot_and_get_max_blob_id(): 新しいソートに置き換える。remove_entry は、コンパクションカタログに
      コンパクション済み WAL ファイルがあるときだけ書く。
* [ ] create_compaction_output(): 新しいソートに置き換え、write_serialized の最も古い版に置き換える指定で書く。
      remove_entry は書かない。値の 1 件ごとのコピーをなくす。
* [ ] BLOB の ID の最大値を、出力からではなく走査の中で記録する (縮約と範囲削除で除くエントリの分も含める。L3 8.1)。
* [ ] コンパクションの入力から現行世代のコンパクション済み WAL ファイルを走査の入力から外し、最後のマージに渡す
      (datastore::compact_with_online()、dblogutil の compaction (dblogutil.cpp:434))。
* [ ] スレッド数: compaction_options の num_worker を LIMESTONE_SORT_THREADS に置き換える。recover_max_parallelism は
      受け付けるが使わない。dblogutil の --thread_num は inspect と repair だけに使う (L3 15)。
* [ ] 既存の compaction 配下、snapshot 配下 (snapshot_impl_test、cursor_impl_test)、log/dblog_scan_test が変えずに通ること。

### ステップ 7: 旧ソートの削除 (S1 メモ 5・6・8 節)

* [ ] sortdb_wrapper、旧ソート専用の関数 (comp_twisted_key()、insert_entry_or_update_to_max()、insert_twisted_entry()、
      split_db_value_and_blob_ids()、create_value_from_db_key_and_value() など) を削除する。
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

* [ ] LIMESTONE_SORT_THREADS と LIMESTONE_SORT_ARENA_SIZE_MB を変えて、起動時のソートの所要時間を測る。測定の条件は
      S1 メモ 3 節と同じにし、旧バージョンの結果 (ソート 18.2 秒) と比べる。
* [ ] 結果と考察をもとに、性能、安定性、使用するリソースが過大でないかを基準に既定値を決める。結果を 5 節に記録する。
* [ ] ソートの比較で呼ぶ関数 (参照型のアクセサ、key_sid の比較関数) を .cpp に置いたことの影響を確かめる。プロファイルで
      呼び出しのコストが目立つ場合に限り、根拠を添えて .h に戻すことを提案する (5 節)。
* [ ] 所有型の move とコピーのたびに参照型を作り直す (解析し直す) ことの費用を確かめる。目立つ場合は、バッファの置き場所が
      変わらない move では作り直さない形を検討する。

### ステップ 10: LE 段階 2

* [ ] 分類の判定関数と、利用箇所の entry_type の列挙の置き換え (LE 9.1)。
* [ ] レプリカの受信で未定義の種別と現れてはならない種別を abort にする (LE 9.1、P21)。
* [ ] アクセサの種別の検査 (LE 9.2)。
* [ ] 新しい名前のアクセサ (serialized_key_sid() など) の追加 (LE 10.3)。旧名は段階 4 まで残す。
* [ ] 型付き版の書き出しの長さの検査 (LE 12)。
* [ ] BLOB の GC の型 blob_gc_entry と、log_entry_container・log_entry_comparator の置き換え、log_entry_container.h の
      順序の文書の修正 (LE 13.1、P19)。
* [ ] UT: LE 15 節の abort する条件、書き出しの観点。

### ステップ 11: LE 段階 3

* [ ] 残りの呼び出し元 (cursor、compaction_carry_writer、レプリカの受信、テスト) を参照型と新しいアクセサに移す。
* [ ] std::istream から読む残りの呼び出し元を新しい読み込みに移す。
* [ ] cursor の move 後の状態の文書 (P20)。

### ステップ 12: LE 段階 4

* [ ] 旧 API を削除する: バイト列版の書き出し、旧名のアクセサ、truncate_value_from_normal_entry、
      make_normal_with_blob_log_entry、static 関数 write_version_epoch_number・write_version_minor_write_version・
      parse_blob_ids、read と read_entry_from、所有型の write(FILE*)。
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
  揃っている」を型の不変条件とする。friend は使わない。形式の定義のヘッダには、形式の記述、長さの定数、key_sid の比較関数、static_assert だけを置く。
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

## 6. 残った課題

<!-- R1 の範囲外として持ち越すものを記録する。 -->
