# オンラインコンパクションと進行中セッション書き込みの競合調査

- 調査日: 2026-07-24
- 対象: /home/umegane/git/limestone (branch feat/rdma-tcpless-replication, commit 3b5cfbda1405e133b0b462e6a09635617bb88633)
- 調査方法: ソースコードの読解のみ (変更なし)

## 結論

バグの疑いあり。rename 直後のローテーション済みファイルに進行中セッションが書き込みを続ける状態は、
コンパクションの読み取り側で「安全に制御されている」とは言えない。具体的には次の 2 点。

1. データロスの疑い (重大)。ローテーション時点で進行中だったセッション (epoch はローテーション epoch
   より大きい) のスニペットは、コンパクションのスキャンで「非 durable」と判定され、ファイル内の
   marker_begin が marker_invalidated_begin (0x06) に物理的に上書きされる (repair_by_mark)。かつ、
   そのファイルはコンパクションカタログの detached_pwals に登録され、以後の起動時スナップショット
   入力から恒久的に除外される。ところがそのセッションはその後正常に end_session し、epoch は
   durable として上位 (shirakami) に通知される。durable 確定済みのデータが、無効化済み・入力除外済み
   のファイルにしか存在しない状態になり、次回再起動時に消失する。
2. 一時的なコンパクション失敗 (軽微)。書き込み途中の marker_begin (9 バイト中 1〜8 バイトのみ可視)
   をスキャンが読むと、直前スニペットが durable の場合 corrupted_durable_entries と誤判定して例外を
   投げ、コンパクション全体が "dblogdir is corrupted" で中断する。カタログ更新前の失敗なので再試行で
   回復しうるが、健全なファイルを破損扱いする誤検知である。

一方、「未 durable な epoch のデータがコンパクション済みファイルに混入する」ことはない (epoch フィルタ
で除外される)。また書き込み途中の通常エントリ (SHORT エントリ) で異常終了することも通常はない。
問題の本質はエントリ混入ではなく、進行中セッションのデータを「無効化・切り離し」してしまう側にある。

## 制御の仕組み (現状の流れ)

### 書き込み側 (前提の再確認)

- `log_channel::begin_session()` は毎セッション `fopen(file_path(), "a")` で開き、128KB の全バッファ
  リングを設定する (src/limestone/log_channel.cpp:72, 76)。begin_session マーカーはこの stdio バッファ
  に書かれ、バッファが満杯になるか end_session の fflush まで OS に出ないことがある。
- `finalize_session_file()` で end_session マーカー書き込み → fflush → fsync → fclose
  (src/limestone/log_channel.cpp:93-111)。セッション中は fd が開いたまま。
- `log_channel::do_rotate_file()` は無条件 rename (src/limestone/log_channel.cpp:255-274)。fd の
  開閉と同期しないため、進行中セッションは rename 後もローテーション済みファイルへ追記し続ける。
- `datastore::rotate_log_files()` は `epoch_id_informed_ >= epoch_id` を待つ
  (src/limestone/datastore.cpp:799-804)。`update_min_epoch_id()` は進行中セッションがあるチャネルに
  ついて `upper_limit = min(upper_limit, working_epoch - 1)` とするので (src/limestone/datastore.cpp:508-515)、
  informed がローテーション epoch に達した時点で進行中のセッションの epoch は必ずローテーション
  epoch より大きい。つまり「rename 時に書き込み中でありうるのは、ローテーション epoch より新しい
  epoch のセッションのみ」は成立している。

### コンパクション入力の選定 (確認点 1)

- `datastore::compact_with_online()` (src/limestone/datastore.cpp:925) は rotate 直後に
  `select_files_for_compaction(result.get_rotation_end_files(), detached_pwals)` を呼ぶ
  (src/limestone/datastore.cpp:961)。
- `select_files_for_compaction` (src/limestone/online_compaction.cpp:39-60) は「ファイル名が pwal で
  始まり、長さ 9 超で、detached_pwals 未登録」のものを選ぶ。長さ 9 超の条件により現用の
  `pwal_XXXX` (9 文字) は除外され、rename 済みの `pwal_XXXX.<unixtime>.<epoch>` だけが選ばれる。
- したがって rename 直後のファイル、すなわち進行中セッションがまだ追記中かもしれないファイルは、
  必ずコンパクション入力に含まれる。入力から除外する仕組み (fd が閉じるのを待つ、informed を再確認
  する等) は存在しない。

### 読み取り実装 (確認点 2)

- `create_compact_pwal_and_get_max_blob_id()` (src/limestone/datastore_snapshot.cpp:387) →
  `create_sorted_from_wals()` (同 :130) → `dblog_scan::scan_pwal_files_throws()` (同 :183)。
- `scan_pwal_files_throws` は fail_fast=true、nondurable=repair_by_mark、truncated=report、
  damaged=report を設定する (src/limestone/dblog_scan.cpp:257-263)。エラー報告関数は
  `log_error_and_throw` で、呼ばれると即例外 (src/limestone/dblog_scan.cpp:36-39)。
- 基準 epoch (ld_epoch) は `dblog_scan::last_durable_epoch_in_dir()` (src/limestone/dblog_scan.cpp:75-105)
  で、スキャン開始時に epoch ファイルから読む。rotation_result.get_epoch_id() ではない点に注意。
  ローテーション後スキャンまでの間に durable が進んでいれば ld_epoch はローテーション epoch より
  大きくなりうる。
- ファイル単位のパースは `dblog_scan::scan_one_pwal_file()` (src/limestone/parse_wal_file.cpp:259)。
  読み取り用ストリームは in|out で開かれ (同 :278)、repair のためにその場で書き込みできる。

## エントリ読み取りと epoch フィルタの詳細 (確認点 2, 4)

- スニペット先頭の marker_begin を読むと、`current_epoch <= ld_epoch` なら valid=true、そうでなければ
  非 durable スニペットとして処理する (src/limestone/parse_wal_file.cpp:317-356)。
- valid=true のスニペット内のエントリだけが `add_entry` (ソート DB への投入 = コンパクション入力)
  に渡る (src/limestone/parse_wal_file.cpp:303-307)。valid=false のスニペットのエントリは読み飛ばす。
- 非 durable スニペットは repair_by_mark により、その場で marker_begin の 1 バイト目を
  marker_invalidated_begin (0x06) に上書きする (`invalidate_epoch_snippet`,
  src/limestone/parse_wal_file.cpp:34-45、呼び出しは同 :333-341)。flush はするが fsync は TODO のまま。
- したがって確認点 4 の答えは (a): 進行中セッションの epoch (> ld_epoch) のエントリは epoch 比較で
  捨てられ、コンパクション済みファイルに未 durable データが混入することはない。fsync 前のデータを
  OS バッファ経由で読んでも、スニペット epoch が非 durable なら採用されない。
- ただし ld_epoch はローテーション epoch ではないため、「ローテーション epoch より新しいが、スキャン
  開始時点で既に durable になっていた epoch」のスニペットは valid となり、コンパクション済み
  ファイルに取り込まれる。この場合セッションは完了済み (durable 記録は全セッション終了・fsync 後に
  しか進まない) なので、取り込み自体は破綻しない。ただしカタログには
  `update_catalog_file(result.get_epoch_id(), ...)` (src/limestone/datastore.cpp:1022) とローテーション
  epoch が記録されるため、カタログの max_epoch_id は実際の内容より小さくなりうる (害は限定的とみられる
  が要注意)。

## 書き込み途中の末尾に遭遇した場合の挙動 (確認点 3, 5)

書き込み側は逐次追記なので、読み取り側から見えるのは常に「論理ストリームの先頭からのプレフィクス」。
途中欠落はなく、末尾が任意のバイト位置で切れた形 (SHORT_* トークン) になる。ケース別の挙動:

- SHORT_normal_entry 等 (通常エントリの途中切れ、src/limestone/parse_wal_file.cpp:369-421):
  - 所属スニペットが valid かつ durable (`current_epoch <= ld_epoch`) → corrupted_durable_entries、
    report_error で例外。ただしこのケースは durable データが fsync 済み・完全に可視のはずなので、
    進行中セッション由来では通常発生しない。
  - 所属スニペットが非 durable (valid=false、直前に repair_by_mark 済み) → truncated=report の分岐で
    pe=broken_after (同 :404-416)。`scan_pwal_files` 側では broken_after でも detached 名 (長さ 9 超)
    のファイルなら fail_fast 例外を投げず黙認する (src/limestone/dblog_scan.cpp:166-172)。
    つまり途中切れの末尾はエラーにならず無視され、コンパクションは続行する。
- SHORT_marker_begin (begin マーカー自体の途中切れ、src/limestone/parse_wal_file.cpp:422-449):
  `!first && current_epoch <= ld_epoch` の判定に「直前スニペットの epoch」が使われる。直前スニペットが
  durable だと corrupted_durable_entries → report_error (= log_error_and_throw) → 例外。
  `create_sorted_from_wals` がこれを捕捉して "dblogdir is corrupted" を投げ
  (src/limestone/datastore_snapshot.cpp:185-191)、`compact_with_online` は失敗ログを出して終了する
  (src/limestone/datastore.cpp:907-909)。切れているのは書き込み途中の新スニペットであって破損では
  ないので、誤検知である。
- スニペットがまだ 1 バイトも可視でない場合 (stdio バッファ内): スキャンはきれいな EOF として正常
  終了する。問題はこの後で、後述のとおりデータはファイルに残るがカタログ上は切り離し済みになる。
- 読み取りと追記の同時実行そのもの: 読み手 (fstream) と書き手 (FILE*) は別 fd で、追記はファイル末尾、
  invalidate は既読のスニペット先頭 1 バイトなので OS レベルの破壊はない。ただし追記中のファイルに
  対して invalidate マークを書き込む、という意味論上の競合を防ぐ制御は何もない。

## バグの疑い: 発生条件・影響・再現シナリオ (確認点 6)

### 問題 A: 進行中セッションのデータロス (重大)

発生条件:
- あるログチャネルのセッション (epoch = e_s) が `rotate_log_files()` の rename をまたいで存続する。
  前述のとおり e_s > ローテーション epoch。begin_session の fopen が rename より前なら、その
  セッションの全書き込みはローテーション済みファイルに入る。
- その後 `compact_with_online()` がそのファイルをスキャンし (スキャン時点の ld_epoch < e_s)、
  カタログの detached_pwals に登録する。

経路は 2 つあり、どちらでも同じ結末になる:
- 経路 A-1 (スニペットが部分的に可視): スキャンが marker_begin (epoch=e_s > ld_epoch) を見つけ、
  repair_by_mark でファイル内の marker_begin を 0x06 に上書きする
  (src/limestone/parse_wal_file.cpp:333-341)。書き手はそれに気づかず追記を続け、end_session まで
  正常完了する。
- 経路 A-2 (スニペットが stdio バッファ内で不可視): スキャンは何も見ずに正常終了。書き手の
  end_session の fflush で、スニペット全体がカタログ登録後のローテーション済みファイルに現れる。

その後:
- セッション完了により epoch_id_informed_ / epoch ファイルが e_s 以上へ進み、上位には e_s が durable
  と通知される (src/limestone/datastore.cpp:496-590)。この時点で誰も invalidate マークや detached
  登録を取り消さない。
- 次回起動時、`assemble_snapshot_input_filenames` は detached_pwals をスナップショット入力から除外する
  (src/limestone/datastore_snapshot.cpp:456-484、呼び出しは同 :536-538)。コンパクション済みファイル
  (pwal_0000.compacted) にも e_s のエントリは入っていない (スキャン時に非 durable として除外済み)。
  経路 A-1 ではさらにスニペット自体が invalidated なので、仮にファイルを読んでもスキップされる
  (src/limestone/parse_wal_file.cpp:358-368)。

影響: durable 確定として ACK したはずのトランザクションのログエントリが、再起動後のスナップショット
から消える。サイレントなデータロス。

再現シナリオ (概念):
1. チャネル C が epoch N+1 で begin_session し、エントリを書き続ける (long-running セッション、
   または高頻度書き込みで rename と重なるだけでもよい)。
2. epoch N まで informed が進んだ状態で ctrl/start_compaction を置き、online_compaction_worker が
   `compact_with_online()` を実行。rotate_log_files が C の pwal を rename する。
3. C のセッションはローテーション済みファイルへ追記継続。コンパクションが同ファイルをスキャンし、
   (可視なら) スニペットを invalidate、カタログに detached 登録して完了する。
4. C が end_session し、epoch N+1 が durable 通知される。
5. プロセスを再起動しスナップショットを再構築すると、手順 1 のエントリが存在しない。

補足: 経路 A-1 は書き手の 128KB バッファが途中で flush される程度の書き込み量が必要。経路 A-2 は
書き込み量によらず、rename をまたぐセッションがあれば起こりうるため、発生確率はむしろ高い。

### 問題 B: 途中切れ marker_begin による誤検知でコンパクション中断 (軽微)

発生条件: 書き手の stdio バッファ flush (128KB 境界) が begin_session マーカー 9 バイトの途中で切れた
状態をスキャンが読み、かつ同ファイルの直前スニペットが durable の場合
(src/limestone/parse_wal_file.cpp:426-428)。

影響: corrupted_durable_entries → 例外 → "dblogdir is corrupted" でコンパクション全体が失敗する。
カタログ更新 (src/limestone/datastore.cpp:1022) の前に失敗するため状態は巻き戻り、後で再試行すれば
成功しうる。実害はコンパクション機会の喪失とエラーログだが、破損していないディレクトリを破損と報告
する点が問題。なお同じ「切れた marker_begin」でも repair 系モード (dblogutil) なら invalidate される
だけで致命化しないが、そのモードは online compaction では使われない。

### まとめ

- 未 durable データの混入 (確認点 4-b) は起きない。epoch フィルタは機能している。
- 途中切れエントリの読み取り (確認点 3, 5) も、detached ファイルの broken_after を黙認する設計に
  より、通常はコンパクションを止めない (例外: 問題 B)。
- ただし「rename 後もセッションが書き込み続けるファイルを、無効化マーク付与とカタログ detached 登録
  の対象にしてしまう」点は制御されておらず、durable 通知済みデータの消失 (問題 A) につながる。
  総合して、既存バグの疑いありと判断する。

## 未確認事項

- 実挙動での再現確認 (本調査はコード読解のみ。特に経路 A-2 のタイミング成立を実機で確認していない)。
- 上位層 (shirakami / jogasaki) がローテーションをまたぐ長いセッションを実際に発生させる頻度。
  epoch 切り替えごとにセッションを区切る運用なら、rename をまたぐ window は 1 セッション分と短いが、
  ゼロにはならない (rotate_log_files は informed 待ち後の rename ループ中に開始された新セッションとも
  競合しうる。begin_session は rename 前の旧名を fopen しうるため)。
- 経路 A-1 の invalidate マークが書き手側の以後の動作 (同一ファイルへの追記) と衝突して I/O エラーに
  なるケースの有無 (別 fd なので通常はないはず)。
- compaction_catalog の max_epoch_id がローテーション epoch で記録される (実内容は ld_epoch まで
  含みうる) ことの、blob GC 境界判定 (src/limestone/datastore.cpp:940) への影響。
- master ブランチとの差分 (本調査は feat/rdma-tcpless-replication の作業ツリーで実施。該当ロジックは
  replication 変更とは独立の共通部分であり、master でも同様と推定されるが未照合)。

## 追記: 検証結果と対策検討の結論 (2026-07-27)

以降は本ドキュメントの調査結果を起点とした検証・議論の結論を記録する。

### 再現確認 (未確認事項の一部解消)

- 問題 A の両経路 (A-1, A-2) を UT で決定論的に再現した。
  - テスト: test/limestone/compaction/online_compaction_inflight_session_test.cpp
    (ブランチ fix/online-compaction-inflight-session、master 5aaf652 から分岐)。
  - 方式: datastore_test の on_rotate_log_files_callback フック (rotate_log_files がローテーション
    epoch を読んだ直後・rename 前に発火) でコンパクションスレッドをブロックし、その窓で
    switch_epoch と進行中セッションの開始・書き込みを行う。
  - A-1 は 128KiB 超の書き込みで stdio バッファを溢れさせて再現。marker_begin が
    marker_invalidated_begin (0x06) に物理上書きされること、detached_pwals 登録、再起動後の
    エントリ消失をすべて確認。A-2 は小書き込みで再現し、WAL には無傷の有効スニペットが残るのに
    detached_pwals 登録により再起動後に読まれないことを確認。
  - 現時点のテストは「バグの痕跡を確認する」形のアサーション (現状でパス) になっており、
    修正後は「あるべき挙動」のアサーションへ反転が必要。
- master ブランチでも再現する (再現テストは master ベースのブランチで実施)。未照合事項は解消。
- repair_by_mark が tglogutil repair 専用という理解は誤りで、scan_pwal_files_throws
  (src/limestone/dblog_scan.cpp:257-263) が db_startup モードとして repair_by_mark を
  ハードコードで設定しており、オンラインコンパクション経路と起動時スナップショット作成経路の
  両方で実行される。実行ログ ("marked invalid ...") でも確認済み。

### 問題の再定義

対策検討の観点から、問題 A/B を次の 2 つに再整理した。

1. close 前のファイルがコンパクション対象になること。
   問題 B (途中切れ marker の誤検知) と、A-1 の「追記中ファイルへの invalidate 書き込み」の
   直接原因。
2. コンパクション対象内の durable epoch 以降の情報が失われること。
   A-1 / A-2 のデータロスの本体。

制約: 外部 I/F 上、ログチャネルには処理中の epoch より大きな epoch (原理上 +100 も) が
書き込まれうる。したがって「コンパクション入力を durable 済みデータに限定する」
「durable epoch の追いつきを待つ」という設計は成立せず、durable epoch 以降のログエントリが
入力に含まれていてもコンパクションが正しく動作する必要がある。

### 検討した対策案

- 案 1 (未 durable スニペットを含むファイルを detached 登録しない): NG。
  compacted ファイルは remove_entry を物理削除する
  (src/limestone/datastore_snapshot.cpp:437-439) ため、吸収済み部分を再読すると削除済み
  エントリが復活する。回避にはコンパクションロジックの大幅修正が必要で非現実的。
  一方 detached 登録すれば未 durable 部分がロストする。ファイル粒度の二択はどちらも不成立。
- 案 1' (carry-over): 入力ファイルは 1 パスで全量消費 (detached 化) し、未 durable スニペットは
  バイト列のまま新しい WAL ファイルへ書き写して引き継ぐ。成立するが、データコピーのコストと、
  コピー完了〜detached 登録の間のクラッシュ窓 (スニペット二重化) の設計が必要。
- 案 2 (カタログに中間ステートを追加): 「一部コンパクション済み」状態と境界オフセットを
  compaction_catalog に登録し、読者は境界以降だけを読む。チャネルのセッションは直列で
  スニペット epoch はファイル内で非減少のため、境界は 1 つで足りる。スキャンを「最初の
  非 durable スニペットで打ち切り、オフセットを報告」とすれば invalidate の問題も自然に消える。
  成立するが、読者全員 (スナップショット組み立て・次回コンパクション・dblogutil・リカバリ・
  backup) が境界を尊重する必要があり修正範囲が広い。尊重漏れが 1 箇所でもあると復活バグが
  再発するリスクがある。
- 案 3 (rename の位置を変える。有力案): rename を rotate_log_files の一括処理から外し、
  ファイルが閉じている瞬間に行う。
  - ローテーション要求時に E = switched を確定し、既存ファイルを持つ各チャネルに rename 保留を
    立てる。
  - end_session: fclose 後に保留があれば rename する (要求時に開いていたセッション、epoch ≤ E)。
  - begin_session: 保留があれば先に旧ファイルを rename してから新しい素の名前のファイルを
    fopen する。これにより要求後に始まるセッション (epoch > E になりうる) は必ず新ファイル側に
    書く。
  - informed ≥ E に達した時点で、まだ rename されていないファイル (要求以降セッションが
    発生しなかったアイドルチャネル) は、end_session とは別に外部から rename する。
  - 効果: ローテーション済みファイルは常に close 済み・完全 (問題 1 と問題 B が構造的に消える)。
    かつローテーション済みファイルのスニペット epoch はすべて ≤ E となるため、informed ≥ E
    待ちの後は入力の全スニペットが durable であり、問題 2 も消える。全量吸収 → detached 登録が
    無条件に正当化され、remove 物理削除の不変条件も保たれる。
  - 外部 I/F 制約とも整合する。durable epoch を超えて先行するのは durable スニペット内の
    エントリの write_version であり、それらは compacted ファイルに write_version ごと保存される。
  - 修正は書き込み側 (log_channel / rotate_log_files) に局所化され、カタログ・スキャナ・
    スナップショット組み立て・リカバリは無修正。新しい永続状態も持ち込まない (保留フラグは
    in-memory で、クラッシュ時は次回起動のリカバリが現行どおり働く)。
  - begin_session / end_session / 外部 rename の間の排他の要否・設計については議論が必要。

### 現状と今後

- 実修正に着手するかは未定。全体への影響を評価してから判断する。
- 再現テストは fix/online-compaction-inflight-session ブランチ上に未コミットで存在する。
