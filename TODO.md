# TODO: R2 ロケータファイルの生成、R3 インデックス反映の読み取りのキー範囲分割 (作業計画)

作業ブランチで使う計画と進捗のメモ。作業しながら更新し、各コミットに含める。作業の記録としてブランチのコミットにだけ残し、
master へのマージ前に本ファイルを削除する。

## 1. 概要

* 目的: コンパクション済み WAL ファイルとスナップショットファイルの書き出し時にロケータファイルを生成し (R2)、
  インデックス反映の n 本の cursor が、ロケータファイルで区切った領域ごとに 2 つのファイルを独立に読む形に置き換える (R3)。
  1 本で読んで n 本のキューへ分配する現行の機構は削除する。
* 設計の正本 (nt-tsurugi-internal の master、da56f13、2026-11-09)
  * L3 (全体) instant-recovery-spec.md: 9 節 (ロケータファイル、領域、cursor、旧バージョンの永続化データ、検証事項)、
    13 節 (測定)、15 節 (互換性)
  * log_entry 再設計 仕様 (以下 LE): 11.2 節 (位置を指定した読み込み)、11.5 節 (書き込み中のファイル)
  * S2 メモ instant-recovery-s2-locator-notes.md: 2 節 (決定事項)、3 節 (形式の細部)、4 節 (記録間隔と R)、
    5.1 節 (R2 の変更箇所)、5.2 節 (R3 の変更箇所)、6 節 (テスト項目)、7 節 (作業順序)、9 節 (申し送り)
* 対象 issue: nt-tsurugi-internal #170 (R2)、#171 (R3)。R3 の完了が CP2。
* 基準コミット: limestone e785c77 (2026-11-02。S2 メモの変更箇所の記述はこのコミットのもの)
* 作業ブランチ: feat/instant-recovery-locator (master e785c77 から分岐)
* 日程 (WBS): R2 2026-11-06〜11-09 (2 人日)、R3 2026-11-10〜11-13 (4 人日)。

## 2. 作業の進め方

* ステップの項目を 1 つ作業し、差分とテスト結果をユーザに見せて確認を受け、指示があったらコミットして次の項目へ移る。
  複数の項目をまとめて進めない。
* 各ステップの終わりで、ビルドが通り、既存のテストが通る状態にする。R2 だけを取り込んだ状態でも動く (S2 メモ 7 節)。
  このために、ステップの順序は S2 メモ 7 節から変えている (5 節の記録)。
* 修正の都度のテストは対象のテストだけを filter で実行する。フルスイートはステップの区切りとコミット前に実行する。
  timeout を付け、出力の伸びを監視する。
* コメントは実装中は日本語で書き、コミット準備で英訳する。
* コミットメッセージは Conventional Commits の英文とし、案に和訳を添えて確認を得てからコミットする。
* 計画からの変更、判断した事項、残った課題は本ファイルの 5 節・6 節に記録する。

## 3. 着手前の確認事項

| # | 事項 | 状態 |
|---|---|---|
| Q1 | PR の単位。R2 と R3 で 1 本にするか、R2 (ステップ 1〜7) と R3 (ステップ 8〜14) で分けるか。R2 だけでも動く設計なので分けられるが、分けると R2 のマージで永続化データの形式バージョンが R3 より先に 9 になる。分ける場合、本ファイルは R2 の最終コミットで削除し、R3 のブランチで再追加する | 未決 |
| Q2 | 形式バージョンを 9 に上げる時期 | 決定済み (S2 メモ 7 節: 生成と移行が揃った R2 の最後。ステップ 5) |
| Q3 | R3 の cursor の変更を shirakami と結合して確かめる手段。limestone の UT (n 本の cursor の過不足のないこと) で足りるか、tsurugidb での結合確認を R3 の中で行うか (結合確認はユーザが実行) | 未決 |

## 4. ステップ

### ステップ 0: 準備

* [x] 作業ブランチを作る。
* [ ] 現状のビルドとフルスイートの結果を記録する (基準)。書きかけの locator/ が GLOB に入っているので、基準は
      master e785c77 の別ビルドで取る。5 節に記録する。

### ステップ 1: locator モジュール (S2 メモ 3 節、5.1 節の新設のモジュール)

* [x] src/limestone/locator/ を新設し、src と test の CMakeLists の GLOB に加える。
  * [x] locator_format: 定数 (ヘッダ 4096、レコード 8、終端 16、マジック 2 種、形式バージョン 1)、名前の変換と
        .loc の判定。
  * [x] locator_settings: record_interval_bytes (32 MiB)、region_records (1)。
  * [x] locator_writer: ヘッダの書き出し、note_entry の記録の規則、close(locatee_size) で終端を書いて fsync、
        build_from_file。書き出しは buffered_output_file で、失敗は limestone_io_exception。build_from_file は
        ロケーティファイルの読み込みが read_error (不正、末尾で不足) を返したら limestone_io_exception (errno なし) とし、
        移行 (ステップ 5) はそれで失敗する。
  * [x] locator: 読み込みと不正の 5 条件の検査 (不正なら ERROR ログにどの条件かを出して limestone_exception)、
        positions()、position(k)、locatee_size()。
* [x] ビルドを通す。
* [x] UT (test/limestone/locator/): 形式 (ヘッダ、レコードの位置、終端のバイト列)、記録の規則 (倍数への到達、
      1 エントリで複数の倍数、レコード 0 が最初のデータ系のエントリ)、レコード 0 個の書き出しと読み込み、
      不正の 5 条件の検出、ロケーティファイルの大きさの照合、build_from_file が書き出し時の生成と同じ内容になること、
      壊れたロケーティファイルで build_from_file が limestone_io_exception を投げること、名前の変換と判定。

### ステップ 2: sort_output_writer の拡張 (S2 メモ 5.1 節の書き出し)

* [x] ロケータファイルのパスと記録間隔を任意で与えられるようにする。write() はデータ系のエントリを書く前に、それまでに
      書いたバイト数 (marker_begin を含む) を note_entry に渡す。keep_remove_entry が偽で書かない remove_entry は渡さない。
      close() はロケーティファイルを閉じた後にロケータファイルを close(書いたバイト数) する (fsync は locator_writer が
      行う。buffered_output_file は ::fsync を直接呼ぶので UT では観測せず、sync_and_close を通ることをコードで確認する)。
* [x] UT: 各レコードの位置から log_entry_reader で読むとデータ系のエントリが得られること、remove_entry を書かないときに
      位置がずれないこと。

### ステップ 3: 名前の判定 (S2 メモ 5.1 節の名前の判定と起動時の検査)

ロケータファイルがまだ存在しない状態で入れても既存の動作を変えないので、公開 (ステップ 4) より先に入れる。

* [ ] dblog_scan::is_wal を、locator_format の判定で .loc で終わる名前を除くように変える。
* [ ] select_files_for_compaction で .loc で終わる名前を除く。
* [ ] compaction_catalog::is_orphan_compaction_file を、コンパクション出力の名前に .loc を付けた名前も対象にし、カタログに
      登録されたコンパクション済み WAL ファイルのロケータファイル以外を孤児とするように変える (ロケーティファイルの
      有無によらない)。is_compaction_output_filename は世代の接尾辞が数字だけであることを求めるので、.loc 付きの名前を
      通す変更が要る。
* [ ] UT: is_wal がロケータファイルを除くこと、inspect が無視すること、通常の起動で .loc がソートの入力に入らず起動に
      失敗しないこと、コンパクションの入力にロケータファイルが選ばれないこと (入力が現行のコンパクション済み WAL
      ファイル 1 つだけなら実行しないこと)、未登録のロケータファイルがロケーティファイルの有無によらず削除されること。

### ステップ 4: 出力の公開 (S2 メモ 4 節、5.1 節の書き出しと出力の公開)

* [ ] datastore_impl に locator_settings とテスト用の設定を加え、create_snapshot_and_get_max_blob_id と
      compact_with_online が作る compaction_options に渡す (ソートの設定と同じ経路)。dblogutil は既定値。
* [ ] compaction_options にロケータファイルの名前と locator_settings を加える。create_compaction_output() は to_dir に
      一緒に書く。
* [ ] create_snapshot_and_get_max_blob_id(): data/snapshot.loc を指定する。スナップショットファイルと同じく add_file しない。
* [ ] datastore::compact_with_online(): 一時ファイルの失敗時の削除にロケータファイルを加える。公開の rename はロケータ
      ファイル、コンパクション済み WAL ファイルの順。カタログの更新の後の add_file にロケータファイルを加える。
      旧世代の削除はロケータファイルを先に削除して subtract_file し、次にコンパクション済み WAL ファイル。
* [ ] dblogutil の compaction: 出力の名前の指定を加える (作業ディレクトリごと rename なので他は変えない)。
* [ ] UT: compaction_test、online_compaction_generation_test (公開後にロケータファイルがあり add_file されていること、
      旧世代のロケータファイルが削除されること、失敗時にロケータファイルも削除されること、テスト用の設定で記録間隔を
      小さくできること)、dblogutil_compaction_test (出力にロケータファイルがあること)、スナップショット作成でロケータ
      ファイルができること、バックアップにロケータファイルが含まれること。

### ステップ 5: 形式バージョンの移行と 9 への更新 (S2 メモ 5.1 節の移行、L3 9.4 節)

生成の条件と書き換えの条件はどちらも「旧バージョンが default_persistent_format_version 未満」なので、既定を 9 に上げるのと
同じステップ (1 コミット) で行う。分けると、8 のまま生成だけが動く中間状態になるか、UT が書けない。

* [ ] manifest::check_and_migrate を「検査」(修復、読み込み、対応バージョンの検査。旧バージョンのマニフェストを返す) と
      「書き換え」(migrate_manifest) に分ける。check_and_migrate は 2 つを合成した関数として残し、既存の manifest_test は
      そのまま通す。
* [ ] check_and_migrate_logdir_format: 検査 → ensure_compaction_catalog → カタログの読み込み → 旧バージョンが
      default_persistent_format_version 未満なら、登録されたコンパクション済み WAL ファイルごとに既存のロケータファイルを
      削除して build_from_file で生成し、ファイルとディレクトリを fsync し、その後にマニフェストを書き換える。既定以上なら
      生成も書き換えもしない。スナップショットファイルは生成の対象にしない。INFO ログ (ファイル名、レコード数)。
* [ ] dblogutil の main で、check_and_migrate_logdir_format を flock の取得の成否の確認の後に移す。
* [ ] default_persistent_format_version を 9 にする。migration_info (ローテーションの要否は 5 以下から) は変えない。
* [ ] 8 を最新、9 を未対応と仮定するテストを直す (未対応の例は 10 にする): log_dir_test の accept_manifest_version_v8、
      reject_manifest_version_v9、rotate_old_rejects_unsupported_data (data_manifest(9))、datastore_impl_test の
      migration_info と get_new_version の期待値。テストデータの既定 (data_manifest(1)) は変えない。
* [ ] UT: log_dir_test、manifest_test、dblogutil_test (8 と 7 以下からの起動で生成されマニフェストが 9 になること、
      移行でスナップショットのロケータファイルを作らないこと、生成の途中で停止した状態からの再起動で作り直されること、
      既存のロケータファイルが捨てられること、dblogutil の各サブコマンドでも生成されること、flock に失敗したときは
      移行しないこと、9 の永続化データでは何もしないこと)。

### ステップ 6: 起動時の存在検査 (S2 メモ 5.1 節の起動時の検査)

移行 (ステップ 5) の後に入れる。移行で生成したロケータファイルはここで add_file する。

* [ ] 起動時の存在検査 (datastore の構築) で、登録されたコンパクション済み WAL ファイルのロケータファイルの存在を確かめ、
      欠けていれば ERROR ログを出して起動を失敗させ、あれば add_file する。
* [ ] 手作りのコンパクション済み WAL ファイルとカタログで datastore を構築する既存テストのフィクスチャにロケータファイルを
      足す (build_from_file を使うテスト補助を 1 つ置く)。候補: compaction_test_fixture.h、offline_compaction_test、
      online_compaction_generation_test、log_dir_test、assemble_snapshot_input_filenames_test、compaction_scenario_test、
      compaction_catalog_test。datastore を構築するものを作業時に絞る。
* [ ] UT: 登録済みのロケータファイルが欠けると起動に失敗すること、移行直後に取ったバックアップにロケータファイルが
      含まれること。

### ステップ 7: R2 の区切り

* [ ] フルスイート。
* [ ] scripts/run-clang-tidy-ci.sh。
* [ ] コメントの英訳 (Q1 で R2 を別 PR にするなら PR の準備もここで)。

### ステップ 8: log_entry_reader の拡張 (S2 メモ 5.2 節)

* [ ] バッファの常用の大きさ (拡張と縮小の基準) を指定する構築子。定数 buffer_size をインスタンスの値に変える。
      既定は 16 MiB。
* [ ] 読み込みの終了位置 (任意)。1 回の read は常用の大きさと終了位置までの残りの小さいほう。終了位置に達したら end。
* [ ] 開いた fd から読む構築子 (fd は dup して reader が所有する)。テスト専用の file_operations の差し替えをこの構築子でも
      使えるようにする (読み込み量の UT 用)。
* [ ] UT: 小さい常用の大きさで 4 KiB を超えるエントリを読めること (拡張が常用の大きさを基準にすること)、終了位置を
      越えて読まないこと (読んだバイト数)、終了位置がエントリの境界のときの end、fd から読めること、unlink の後も
      読めること。既存の log_entry_reader_test は通ること。

### ステップ 9: cursor_impl の範囲 (S2 メモ 5.2 節)

* [ ] 範囲を与える構築子: 2 ファイルそれぞれの開始位置と終了位置、開始キー (任意)、終了キー (任意)。両ファイルについて
      開始キーより小さいエントリは読み飛ばし、終了キー以上で止める。終了位置は reader に渡す。
* [ ] reader の構築を 1 つの関数にまとめる (fd、常用の大きさ、終了位置)。状態の関数 (LE 11.5 節) は R8 でこの関数に足す。
* [ ] 範囲なしの構築子、マージの規則、clear_storage、昇順の検査は変えない。既存の cursor_impl_test は通ること。
* [ ] UT: 範囲の境界 (開始キーと等しいキー、終了キーと等しいキー、開始キーなし、終了キーなし)、副ファイルだけの
      小さいキー、同じキーが両方にある場合。

### ステップ 10: プローブと region_cursor (S2 メモ 5.2 節)

* [ ] プローブ: 常用の大きさ 4 KiB の reader を fd から作り、1 件読んで key_sid をコピーする。読めない、またはデータ系で
      なければ ERROR ログを出して limestone_exception。
* [ ] region_cursor (src/limestone/ 直下に cursor_impl と並べて置く。cursor_impl_base を実装し、cursor.h に friend は
      足さない): 共有の割り当て (atomic のカウンタ) から領域を取り、S2 メモ 5.2 節の 4 項目で範囲を計算して cursor_impl を
      作り、読み終えたら次の領域へ。範囲の計算は、主ファイルの開始位置と終了位置 (次のレコードがなければ末尾)、開始キーと
      終了キー (先頭と最後の領域ではなし)、副ファイルの開始位置 (開始キーより小さい最後のレコード。なければ 0) と終了位置
      (終了キー以上の最初のレコード。なければ末尾)。
* [ ] region_cursor を api::cursor に包む手段: 既存の friend である cursor_impl に、cursor_impl_base を受け取る
      create_cursor の多重定義を加え、get_partitioned_cursors はそれを使う。
* [ ] UT (test/limestone/snapshot/): 領域の範囲の計算 (開始キー・終了キー、主ファイルの終了位置、副ファイルの読み始めと
      終了位置、該当するレコードがない場合、先頭と最後の領域、R = 1 と R ≥ 2)、プローブ (4 KiB 超、データ系でない
      エントリで失敗すること)。

### ステップ 11: snapshot_impl::get_partitioned_cursors の置き換え (S2 メモ 5.2 節)

* [ ] 2 ファイルを 1 回開き (fd を n 本で共有し、最後の region_cursor の破棄で閉じる)、locator を読み、レコード数の多い方を
      主ファイルにし (同数ならコンパクション済み WAL ファイルが主。コンパクション済み WAL ファイルがなければ
      スナップショットファイルが主で副なし)、領域数を ceil(主のレコード数 ÷ R) として n 本の region_cursor を返す。
      主のレコードが 0 個なら全 cursor がすぐ終端。partitioned_called_ の契約は残す。locator が不正なら limestone_exception。
* [ ] R (region_records) を datastore_impl から snapshot の private 構築子 (公開ヘッダ) を通して snapshot_impl に渡す。
      公開ヘッダから内部ヘッダの locator_settings は include できないので、R だけを std::size_t で渡す。テスト用の設定で
      小さい値を与えられるようにする。
* [ ] 既存の snapshot_impl_test の手作りファイルにロケータファイルを足す (ステップ 6 のテスト補助を使う)。
* [ ] get_cursor() と BLOB GC の my_cursor は変えない。

### ステップ 12: 分配の機構の削除 (S2 メモ 5.2 節)

* [ ] src/limestone/partitioned_cursor/ と test/limestone/partitioned_cursor/ を削除し、両 CMakeLists の GLOB の行、
      cursor.h の partitioned_cursor_impl の前方宣言と friend 宣言を削除する。
* [ ] docs/internal/get_partitioned_cursors.md を region_cursor の説明に書き換える。

### ステップ 13: R3 の総括テスト (S2 メモ 6 節 R3、L3 9.5 節)

* [ ] 必須: n = 1、2、3、8 で、n 本の cursor の出力をすべて集めたものが get_cursor() の出力と件数・内容ともに一致し、
      各 cursor の出力がキーの昇順であること。記録間隔を小さくして領域を多数作り、R は 1 と 2 以上 (主のレコード数が
      R で割り切れない場合を含む) の両方で行う。次の場合を含める。
  * 主のレコードが 0 個の場合、領域が 1 つの場合。
  * 先頭の領域に、副ファイルにだけある小さいキーがある場合。
  * 副ファイルに、領域の境界のキー (開始キー・終了キー) と等しいキーがある場合。
  * 主ファイルがスナップショットファイルの場合、コンパクション済み WAL ファイルの場合、レコード数が同数の場合。
  * 同じキーが両方にある場合 (スナップショット側が採られること)、remove_entry、clear_storage。
  * コンパクション済み WAL ファイルがない場合。
  * キーの分布を乱数で変えて複数回。
* [ ] 読み込み量: 領域ごとの reader が終了位置を越えて読まないこと (file_operations の差し替えで読んだバイト数を数える)。
* [ ] 旧世代の削除との競合: get_partitioned_cursors() の後にコンパクション済み WAL ファイルを削除しても残りの領域を
      読めること。
* [ ] 不正なロケータファイルで snapshot_impl::get_partitioned_cursors() が limestone_exception を投げること
      (公開 API は abort するので snapshot_impl の層で確かめる)。region_cursor の next() でのプローブの失敗も含める。
* [ ] 既存の snapshot_impl_test と cursor_impl_test が (ロケータファイルの用意を加えて) 通ること。

### ステップ 14: 仕上げ

* [ ] フルスイート。scripts/run-clang-tidy-ci.sh。
* [ ] コメントの英訳。
* [ ] 本ファイルの 5 節・6 節を整理し、残った課題を issue または申し送りに移す。本ファイルを削除する。
* [ ] PR を作る (Co-Authored-By・Generated with のフッターは付けない)。
* [ ] マージ後: tsurugidb への反映 (ユーザが実施。limestone の master を push → tsurugidb で submodule update --remote →
      tsurugidb の規約に沿ってコミット・push)。tateyama の設定の文書に加える項目はない (S2 メモ 8 節)。

## 5. 記録

* 2026-11-09: ステップ 1 のコードを書いた段階でユーザの指示により中断し、本ファイルを作成した。その後ステップ 1 の
  ビルドと UT (33 件) を終えた。
* 2026-11-09: 本ファイルを /crv でレビューし (観点: nt-tsurugi-internal の設計との合致)、次を変えた。
  * ステップの順序を S2 メモ 7 節から変えた。名前の判定を公開より前に (公開の後に .loc が WAL として走査されるのを
    避ける)、存在検査を移行の後に (移行で生成した分の add_file と、既存フィクスチャの修正をまとめる)、移行と形式
    バージョンの更新を 1 ステップに (生成と書き換えの条件がどちらも既定未満のため)。
  * 公開ヘッダ snapshot.h には locator_settings でなく R を std::size_t で渡す (内部ヘッダを include できないため)。
  * region_cursor を api::cursor に包むのは cursor_impl の create_cursor の多重定義で行う (friend を足さないため)。
  * build_from_file がロケーティファイルの read_error に当たったときは limestone_io_exception (errno なし) とする
    (S2 メモ 5.1 節に契約がなかった。設計側への反映は 6 節)。
  * 「テストデータの形式バージョンを 9 にする」(S2 メモ 6 節) は、既定の data_manifest(1) は変えず、8 を最新・9 を未対応と
    仮定するテストだけを直す、と読み替えた。

## 6. 残った課題

* S2 メモ 9 節の申し送り (R2・R3 では扱わない): 移行の途中で停止した永続化データを旧バージョンで起動したときの失敗の
  仕方 (L3 15 節で扱う予定。現時点では未記載)、dblogutil repair --cut がコンパクション済み WAL ファイルを切り詰めたときの
  ロケータファイルの不整合。
* 記録間隔と R の値の決定は R11 (L3 13 節)。L3 10.1 節の量の測定で .loc を除くのは R4。
* S2 メモ 5.1 節への追記候補: build_from_file の read_error の扱い (5 節の記録)。
* buffered_output_file は sort/ にあるが、locator からも使うので sort 固有ではなくなった。src/limestone/ 直下へ移す候補
  (別コミット)。
* 読み込み側 (locator) はレコードが狭義単調増加でロケーティファイルの大きさ未満であることを検査しない (設計の不正条件は
  5 つ)。R3 で領域の境界に使う側が前提にするなら、検査を足すかは設計側の判断。
