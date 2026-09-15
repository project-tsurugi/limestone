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

phone-bill のスクリプトは `$TSURUGI_DIR` (既定 `~/tsurugi/tsurugi`) を参照する。
今回ビルドしたものへシンボリックリンクを張り替える。

```bash
# 戻すときのために現在の向き先を控える (既定では ~/tsurugi/tsurugi-1.12.0-SNAPSHOT-202607270548-acf615e)
ls -l ~/tsurugi/tsurugi

ln -snf /home/umegane/opt/tsurugi-1.13.0-SNAPSHOT-202609150728-4ebdb20 ~/tsurugi/tsurugi
ls -l ~/tsurugi/tsurugi
```

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

```bash
cd ~/git/phone-bill-benchmark/scripts
CONF=config-cb-small/04-OCC-T04-with-online-app
~/phone-bill/bin/run CreateTestData $CONF
```

> `config-cb-small/` 配下の設定は**ディレクトリではなくファイル**である。
> この設定は `thread.count=4` / `transaction.option=OCC` / `dbms.type=ICEAXE` で、
> オンラインアプリの各 `*.records.per.min=-1` (= 無制限に全力で流す) が効く。
> **オンラインアプリが常時トランザクションを流し続けるため、ローテーション境界に
> 進行中セッションがかかる確率が上がり、carry が出やすくなる**。
> 負荷を上げたい場合は `06-ONLINE-T16-BATCH-T64` 等のスレッド数が多い設定に替える。

### 3. 負荷をかけながらコンパクションを繰り返す

**端末 A** — 負荷を継続的にかける。

```bash
cd ~/git/phone-bill-benchmark/scripts
./run_batch_continuously.sh config-cb-small/04-OCC-T04-with-online-app
```

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
(`config-cb-small/06-ONLINE-T16-BATCH-T64` 等)、あるいはコンパクション指示の間隔を
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
