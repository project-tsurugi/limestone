# issue #139 結合テスト手順 (回帰テストとしての短時間実行)

対象: limestone `fix/issue139-rotation-compaction` (b9b7b0c)
作成: 2026-09-15

## 位置づけ

TODO.md の C2。**回帰テストとしての短時間実行**であり、負荷中のコンパクションを
数回実行して carry の生成を観測し、データの健全性を確認することを目的とする。
環境を用意して長時間走らせる連続運転テストは本手順の対象外。

確認したいのは次の 3 点。

1. 稼働中のオンラインコンパクションが完走し、世代方式のファイル (`pwal_0000.compacted.<N>` /
   `pwal_0000.carry.<N>`) が正しく作られること
2. ローテーション境界を越える進行中セッションがあるとき carry が生成されること (本 issue の本丸)
3. コンパクションを挟んだ再起動でデータが失われないこと

## 環境

| 項目 | 値 |
| --- | --- |
| tsurugidb | `~/opt/tsurugi-1.13.0-SNAPSHOT-202609150728-4ebdb20` (limestone b9b7b0c) |
| ベンチマーク | `~/git/phone-bill-benchmark` |
| phone-bill インストール先 | `~/phone-bill` |
| ログ出力先 | `~/logs` |

### 準備 1: tsurugidb の切り替え

サーバとクライアントで**参照経路が 2 つあり、両方を切り替えないとバージョンが食い違う**。

| 経路 | 参照元 | 既定の向き先 |
| --- | --- | --- |
| サーバ (tgctl 等) | phone-bill の `env.sh` の `$TSURUGI_DIR` | `~/tsurugi/tsurugi` |
| クライアント (tsubakuro の `libtsubakuro.so`) | `~/.bashrc` の `$TSURUGI_HOME` | `~/opt/tsurugi` |

`~/tsurugi/tsurugi` だけを張り替えると、サーバは新しいビルド・クライアントは
`~/opt/tsurugi` が指す古いビルド、という組み合わせになる。

```bash
# (1) サーバ側: 戻すときのために現在の向き先を控えてから張り替える
ls -l ~/tsurugi/tsurugi
ln -snf /home/umegane/opt/tsurugi-1.13.0-SNAPSHOT-202609150728-4ebdb20 ~/tsurugi/tsurugi
ls -l ~/tsurugi/tsurugi

# (2) クライアント側: TSURUGI_HOME をこの検証の間だけ上書きする
#     (~/opt/tsurugi のリンク自体は PATH にも使われる環境全体の既定なので変更しない)
#     負荷をかける端末・コンパクションを指示する端末の両方で実行すること
export TSURUGI_HOME=/home/umegane/opt/tsurugi-1.13.0-SNAPSHOT-202609150728-4ebdb20
```

> 設定できたかは `echo $TSURUGI_HOME` で確認する。
> 実行後は phone-bill のログの
> `loading native library by TSURUGI_HOME: <path>/lib/libtsubakuro.so` でも確認できるが、
> `run_batch_continuously.sh` はクライアントログを**標準出力にしか出さない**
> (`etc/logback_for_continuously.xml` が ConsoleAppender のみ) ため、
> 後から grep するには下記のように `tee` でファイルに落とす。

### 準備 2: ログレベル

carry の生成有無は `VLOG_LP(log_info)` (= レベル 30) で出力される。
`~/.phonebill` の現在値が `TSURUGI_LOG_LEVEL=30` なので**変更不要**。

```
compaction generation <N> committed (carry: yes|no)
```

### 準備 3: phone-bill のインストール

`~/phone-bill` に既にインストール済み。tsubakuro の更新を取り込む場合のみ再インストールする。

```bash
~/git/phone-bill-benchmark/scripts/install.sh
```

## 手順

### 1. 初期化と起動

データベースファイルを消して起動する。

```bash
cd ~/git/phone-bill-benchmark/scripts
./tinit.sh
```

ログディレクトリ (`~/tsurugi/tsurugi/var/data/log`) が作り直され、tsurugidb が起動する。

### 2. テストデータ生成

まず設定ファイルを用意する。`04-OCC-T04-with-online-app` をそのまま使うと
**負荷スクリプトが数周で停止する**ため (理由は下記)、`MasterDeleteInsertApp` だけを
止めた設定を作る。

```bash
cd ~/git/phone-bill-benchmark/scripts/config-cb-small
sed 's/^master\.delete\.insert\.records\.per\.min=.*/master.delete.insert.records.per.min=0/' \
    04-OCC-T04-with-online-app > 04-OCC-T04-issue139
grep -E '^(master|history)\.' 04-OCC-T04-issue139
```

```bash
cd ~/git/phone-bill-benchmark/scripts
CONF=config-cb-small/04-OCC-T04-issue139
~/phone-bill/bin/run CreateTestData $CONF
```

> **なぜ `MasterDeleteInsertApp` だけ止めるのか**
>
> phone-bill は起動のたびに contracts をフルスキャンしてブロック情報を作り直し
> (`DbContractBlockInfoInitializer.init`)、**ブロック内のレコード数がブロックサイズと
> 完全一致したブロックだけを active** とする (`checkBlockFilled`)。active が 0 件だと
> `PhoneBill.createOnlineApps` が
> `IllegalStateException: Insufficient test data, create test data first.` を投げる。
>
> オンラインアプリ 4 種のうち contracts の件数を変えるのは `MasterDeleteInsertApp` だけで、
> delete と insert が別トランザクションのため「delete 済み・insert 前」の瞬間にスキャンすると
> 1 件足りず、そのブロックが active から外れる。残る 3 種
> (`MasterUpdateApp` = update のみ、`HistoryUpdateApp` / `HistoryInsertApp` = history 対象) は
> contracts の件数に影響しない。
>
> したがって `MasterDeleteInsertApp` のみ止めれば停止は防げ、**残り 3 種が常時
> トランザクションを流し続けるので、ローテーション境界に進行中セッションがかかる状況
> (= carry が出る条件) は維持される**。
>
> `config-cb-small/` 配下の設定は**ディレクトリではなくファイル**である。
> 元の設定は `thread.count=4` / `transaction.option=OCC` / `dbms.type=ICEAXE` で、
> オンラインアプリの各 `*.records.per.min=-1` (= 無制限に全力で流す) が効く。
> 負荷を上げたい場合は `06-ONLINE-T16-BATCH-T64` 等のスレッド数が多い設定を同じ要領で加工する。

### 3. 負荷をかけながらコンパクションを繰り返す

**端末 A** — 負荷を継続的にかける。

```bash
export TSURUGI_HOME=/home/umegane/opt/tsurugi-1.13.0-SNAPSHOT-202609150728-4ebdb20
cd ~/git/phone-bill-benchmark/scripts
./run_batch_continuously.sh config-cb-small/04-OCC-T04-issue139 2>&1 | tee ~/logs/phonebill-client.log
```

> クライアントログは標準出力にしか出ないので `tee` でファイルにも残す。
> 環境が揃っているかは `grep "loading native library" ~/logs/phonebill-client.log` で確認する。

**端末 B** — 負荷が走っている間にコンパクションを数回指示する。

```bash
TSURUGI=~/tsurugi/tsurugi
DBLOG=$TSURUGI/var/data/log

for i in $(seq 1 5); do
  echo "=== compaction request $i ==="
  $TSURUGI/bin/tgcmpct_logs.sh $DBLOG
  sleep 30
  ls -la $DBLOG/ | grep -E 'compacted|carry'
  grep -E 'compaction generation .* committed' ~/logs/tsurugidb.log | tail -3
done
```

> `tgcmpct_logs.sh` は `$DBLOG/ctrl/start_compaction` を touch して指示する。
> コンパクションは非同期に走るので、`sleep` で完了を待ってから観測する。
> 30 秒で足りなければ延ばす。

### 4. 観測

各コンパクション後に次を確認する。

```bash
TSURUGI=~/tsurugi/tsurugi
DBLOG=$TSURUGI/var/data/log

# 世代付きファイルとカタログ
ls -la $DBLOG/ | grep -E 'compacted|carry'
cat $DBLOG/compaction_catalog

# carry の生成有無
grep -E 'compaction generation .* committed' ~/logs/tsurugidb.log

# エラーが出ていないこと
grep -E 'ERROR|FATAL|corrupted' ~/logs/tsurugidb.log
```

**期待する結果**

| 観点 | 期待 |
| --- | --- |
| compacted ファイル名 | 1 回目以降 `pwal_0000.compacted.1`, `.2`, ... と世代が上がる |
| carry ファイル | 生成された世代では `pwal_0000.carry.<N>` が存在する |
| カタログ | `GENERATION <N>` 行がある。carry ありの世代は `CARRY_FILE` 行がある |
| 旧世代のファイル | コミット後に削除され、残らない |
| ログ | `compaction generation <N> committed (carry: yes\|no)` が出る |
| エラー | `ERROR` / `FATAL` / `dblogdir is corrupted` が出ない |

**carry が 1 度も `yes` にならない場合**: 進行中セッションがローテーション境界に
かからなかったということ。スレッド数の多い設定に替える
(`06-ONLINE-T16-BATCH-T64` を同じ要領で加工する)、あるいはコンパクション指示の間隔を
詰めて試す。**carry なしでも世代方式そのものは検証できる**が、本 issue の本丸の
確認にはならないため、最低 1 回は `carry: yes` を観測したい。

### 5. データ健全性の確認

負荷を止めて、コンパクションを挟んだ再起動でデータが残っていることを確認する。

```bash
# 端末 A の run_batch_continuously.sh を Ctrl-C で停止

cd ~/git/phone-bill-benchmark/scripts
TSURUGI=~/tsurugi/tsurugi
DBLOG=$TSURUGI/var/data/log

# 再起動前に carry ファイルが実在することを確認する
# carry は次回のコンパクションで吸収されるため、手順 3 のループ最終回で carry が
# 生成されていないとここには残っていない。無ければ手順 3 をもう一巡する
ls -la $DBLOG/ | grep carry
grep CARRY_FILE $DBLOG/compaction_catalog

# 再起動前の件数
$TSURUGI/bin/tgsql --exec -c ipc:phone-bill "select count(*) from history"
$TSURUGI/bin/tgsql --exec -c ipc:phone-bill "select count(*) from contracts"

# 再起動 (データは消さない)
./tstop.sh
./tstart.sh

# 再起動後の件数 — 上と一致すること
$TSURUGI/bin/tgsql --exec -c ipc:phone-bill "select count(*) from history"
$TSURUGI/bin/tgsql --exec -c ipc:phone-bill "select count(*) from contracts"

# 再起動後のログ (tstart.sh は tsurugidb.log のリンクを張り替えないため新しいファイルを見る)
RESTART_LOG=$(ls -t ~/logs/tsurugidb-*.log | head -1)
grep -E 'ERROR|FATAL|corrupted' $RESTART_LOG
```

> `tstart.sh` はデータを消さずに起動する (`tinit.sh` は消す)。
> 起動時に carry ファイルがスナップショット入力として読まれるため、
> **carry を生成させた後の再起動が本 issue の要**。

**期待する結果**

- 再起動が成功する (孤児削除・カタログ読み込みでエラーが出ない)
- 件数が再起動の前後で一致する
- 起動ログに `ERROR` / `FATAL` が出ない

carry が無い状態で再起動しても「コンパクション後の再起動」の確認にはなるが、
本 issue の要である「carry がスナップショット入力として読まれる」経路は通らない。
報告時にはどちらだったかを明記する。

### 6. 後始末

```bash
# tsurugidb 停止
cd ~/git/phone-bill-benchmark/scripts && ./tstop.sh

# tsurugidb のシンボリックリンクを準備 1 で控えた向き先に戻す
ln -snf tsurugi-1.12.0-SNAPSHOT-202607270548-acf615e ~/tsurugi/tsurugi
ls -l ~/tsurugi/tsurugi

# TSURUGI_HOME は export しただけなので端末を閉じれば既定 (~/opt/tsurugi) に戻る
```

## 記録する内容

結果を報告する際は次を添える。

1. `compaction generation ... committed` の全行 (carry の yes/no がわかる)
2. 最終的な `compaction_catalog` の内容
3. コンパクション後の `ls` (世代付きファイルの様子)
4. 再起動前後の件数
5. `ERROR` / `FATAL` / `corrupted` の grep 結果 (空であること)

## 備考

- オンライン blob GC は本ブランチで無効化されている (issue #144 の暫定対応、コミット 368f140)。
  したがって稼働中に blob ファイルは回収されず、再起動時の GC でのみ回収される。
  blob 容量が増え続けても本手順では**異常ではない**
- `tglogutil inspect` は開発中の機能であり、E ログが出ても無視してよい

---

## 実施結果 (2026-09-15)

tsurugidb `1.13.0-SNAPSHOT-202609150728-4ebdb20` (limestone `0ab3b0d`) で実施。
設定は `config-cb-small/04-OCC-T04-issue139` (上記手順で生成)、コンパクション指示 5 回。

### コンパクションの実行

| 世代 | compacted | carry |
| --- | --- | --- |
| 1 | `pwal_0000.compacted.1` (71 KB) | no |
| 2 | `pwal_0000.compacted.2` (92 MB) | **yes** (26,520 B) |
| 3 | `pwal_0000.compacted.3` (108 MB) | **yes** (26,588 B) |
| 4 | `pwal_0000.compacted.4` (124 MB) | **yes** (339 B) |
| 5 | `pwal_0000.compacted.5` (139 MB) | **yes** (26,319 B) |

```
compaction generation 1 committed (carry: no)
compaction generation 2 committed (carry: yes)
compaction generation 3 committed (carry: yes)
compaction generation 4 committed (carry: yes)
compaction generation 5 committed (carry: yes)
```

- **5 世代中 4 回 carry が生成された**。ローテーション境界を越える進行中セッションの
  スニペットを carry に退避する経路が実際に動作している
- 世代番号は 1→5 と単調増加し、旧世代の compacted / carry は各コミット後に削除された
- 負荷スクリプトはコンパクション 5 回を通して停止せず動き続けた
- `ERROR` / `FATAL` / `corrupted` は 0 件

### 再起動時の状態 (carry あり)

再起動前のカタログ:

```
COMPACTED_FILE pwal_0000.compacted.5 1
GENERATION 5
CARRY_FILE pwal_0000.carry.5
MAX_EPOCH_ID 42266
```

**carry がカタログに記録され、ファイルも実在する状態で再起動**した。

| テーブル | 再起動前 | 再起動後 |
| --- | --- | --- |
| contracts | 1,000 | 1,000 |
| history | 670,800 | 670,800 |
| billing | 132 | 132 |

- 全テーブルで件数が一致。**carry に退避されたエントリを含めてデータロスなし**
- 再起動は正常終了 (`successfully shutdown` → `RUNNING`)、起動ログにエラーなし
- 起動後は `pwal_0000.compacted.5` と `pwal_0000.carry.5` が残り、detached_pwals は
  全削除された正しい状態。carry は次回コンパクションで吸収されるためこの時点では残る

本 issue の要である「**carry がスナップショット入力として読まれる**」経路を実機で確認できた。

### 補足: 設定から MasterDeleteInsertApp を外した経緯

当初 `04-OCC-T04-with-online-app` をそのまま使ったところ、負荷スクリプトが数周で
`IllegalStateException: Insufficient test data, create test data first.` で停止した。

調査の結果、limestone のデータロスではなく phone-bill 側の性質だった。
`DbContractBlockInfoInitializer.init` は起動のたびに contracts をフルスキャンして
ブロック情報を作り直し、`checkBlockFilled` が「ブロック内のレコード数 == ブロックサイズ」の
ブロックだけを active にする。オンラインアプリ 4 種のうち contracts の件数を変えるのは
`MasterDeleteInsertApp` だけで、delete と insert が別トランザクションのため
「delete 済み・insert 前」の瞬間にスキャンすると 1 件足りず、そのブロックが active から
外れて active 0 件 → 例外、という流れだった (実際に停止時の contracts は 999 件、
`CreateTestData` が作るのは 1,000 件)。

`MasterDeleteInsertApp` のみ止めた設定に変えたところ、負荷は停止せず、残る 3 種が
トランザクションを流し続けるため carry の生成率はむしろ上がった (2/4 → 4/5)。
