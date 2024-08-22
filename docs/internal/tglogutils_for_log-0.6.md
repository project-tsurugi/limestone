# tglogutilsのLimestone LOG-0.6対応

## テスト用のデータファイルの作成


* initial.tar.gz
    * tsurugiが起動したが、まだPWALが書かれていない状態
* log-not-comacted.tar.gz
    * PWALが書き込まえたがコンパクションが行われていない状態
* log-compacted.tar.gz
    * コンパクションが行われたが、コンパクション後にPWALの書き込みが行われていない状態
* log-compacted2.tar.gz
    * コンパクションが行われ、コンパクション後にPWALの書き込みが行われた状態
* log-delete-detached.tar.gz
    * 削除可能なPWALが削除され、それ以降にPWALの書き込みが行われていない状態
* log-delete-detached2.tar.gz
    * 削除可能なPWALが削除され、その後にPWALの書き込みが行われた状態


## サブコマンド

以下のサブコマンドが存在する

* repair
* compaction
* inspect

## 作業方針

* 前テストデータに対して全てのサブコマンドを実行し、LOG-0.6のデータファイルに対して、処理内容が適切化か判断する。
  * 処理後のログディレクトリの内容が想定通りか
  * 処理顔にtsurugiが問題なく起動できるか
* 適切でない場合
  * 適切に処理できるように修正する
  * LOG-0.6のデータは処理できないというメッセージを出力してコマンドを異常終了する
  * なにもしない(inspectの場合は選択肢になる)

## compaction

* initial.tar.gz
    * 特に問題ないと思われる変化
        * compactionにより、ディレクトリctrl, dataが消える
        * pwal_0000 が消え、pwal_0000.compactedが作成される
        * pwal_0000.compactedが作成される
    * tsurugidbは問題なく起動する

* log-not-comacted.tar.gz
    * PWALが書き込まえたがコンパクションが行われていない状態
* log-compacted.tar.gz
    * 特に問題ないと思われる変化
        * compactionにより、ディレクトリctrl, dataが消える
        * pwal_0000.compactedのタイムスタンプが更新されるが、サイズは変わらない。
          * テストデータは同じ値で更新されているので、圧縮後のサイズが変わらないのは想定通り
          * サイズが変わらないだけでなくバイナリが完全に一致する
    * tsurugidbは問題なく起動する
* log-compacted2.tar.gz
    * log-compacted.tar.gzと同じ状態になる
    * tsurugidbは問題なく起動する
    * log-delete-detached.tar.gz
    * log-compacted.tar.gzと同じ状態になる
    * バイナリまで完全一致 => Tsurugidbの起動確認せず
* log-delete-detached2.tar.gz
    * log-compacted.tar.gzと同じ状態になる
    * tsurugidbは問題なく起動する

* 追加検証
  * コンパクションを行うと、epochが0になるが、問題ないか
  * log-delete-detached.tar.gz を展開
  * この状態で、compaction_catalogのMAX_EPOCH_ID 9459
  * tglogutilでコンパクションを行う
  * compaction_catalogのMAX_EPOCH_IDが0になる。
    * コンパクションにより、compaction_catalogが更新され正しい値になるので問題ない。=> いや問題あり


## inspect

* ファイルを修正しないので、基本的に問題ないはず

* initial.tar.gz
    * 実行結果
    * persistent-format-versionが正しく表示されていない => 要対応、おそらくmanifestファイルを見ていない
```
W0822 16:19:05.785152 80234 dblogutil.cpp:335] WARNING: subcommand 'inspect' is under development
dblogdir: "log"
persistent-format-version: 1
durable-epoch: 2
max-appeared-epoch: 2
count-durable-wal-entries: 1
status: OK
```
* log-not-comacted.tar.gz
```
W0822 16:21:43.293540 81494 dblogutil.cpp:335] WARNING: subcommand 'inspect' is under development
dblogdir: "log"
persistent-format-version: 1
durable-epoch: 9461
max-appeared-epoch: 9461
count-durable-wal-entries: 1299932
status: OK
```
* log-compacted.tar.gz
```
umegane@ubuntu22:~/tsurugi/tsurugi/var/data$ ../../bin/tglogutil inspect log
W0822 16:24:33.794238 82737 dblogutil.cpp:335] WARNING: subcommand 'inspect' is under development
dblogdir: "log"
persistent-format-version: 1
durable-epoch: 9461
max-appeared-epoch: 9461
count-durable-wal-entries: 2201462
status: OK
```
* log-compacted2.tar.gz

```
umegane@ubuntu22:~/tsurugi/tsurugi/var/data$ ../../bin/tglogutil inspect log
W0822 16:51:14.676712 95366 dblogutil.cpp:335] WARNING: subcommand 'inspect' is under development
dblogdir: "log"
persistent-format-version: 1
durable-epoch: 85067
max-appeared-epoch: 85067
count-durable-wal-entries: 3500892
status: OK
```
* log-delete-detached.tar.gz
```
umegane@ubuntu22:~/tsurugi/tsurugi/var/data$ ../../bin/tglogutil inspect log
W0822 16:52:20.373893 95895 dblogutil.cpp:335] WARNING: subcommand 'inspect' is under development
dblogdir: "log"
persistent-format-version: 1
durable-epoch: 85067
max-appeared-epoch: 85067
count-durable-wal-entries: 2200960
status: OK
```
* log-delete-detached2.tar.gz
```
W0822 16:53:53.960847 96624 dblogutil.cpp:335] WARNING: subcommand 'inspect' is under development
dblogdir: "log"
persistent-format-version: 1
durable-epoch: 85067
max-appeared-epoch: 85067
count-durable-wal-entries: 901530
status: OK
```

## repair

* initial.tar.gz
    * tsurugiが起動したが、まだPWALが書かれていない状態
  * リペアにより、pwal_0000がpwal_0000.01724315938528.0にリネームされる。
  * tsurugiは問題なく起動する
  * 起動後のコンパクションも動作する。
* log-not-comacted.tar.gz
    * PWALが書き込まえたがコンパクションが行われていない状態
    * リペアにより全てのPWALファイルがローテーションションされる
    * tsurugiは問題なく起動し、テーブルも見れる
* log-compacted2.tar.gz
    * コンパクションが行われ、コンパクション後にPWALの書き込みが行われた状態
    * 未ローテートのPWALがローテートされ、ローテート済みのPWALはローテートされない
      * tsurugiは問題なく起動する
    * リペア後、オンラインコンパクションを行っても問題ないか
    * repairされたPWALがローテーションされていて問題ない



* log-delete-detached.tar.gz
    * 削除可能なPWALが削除され、それ以降にPWALの書き込みが行われていない状態
* log-delete-detached2.tar.gz
    * 削除可能なPWALが削除され、その後にPWALの書き込みが行われた状態



## 対応すべき問題

* おそらくdetached pwalがオフラインコンパクションの対象になっている。
  * たんにローテーションしただけのPWALはオフラインコンパクションの対象
  * コンパクション済みのPWALは、コンパクションの対象外
    * オフラインコンパクションが遅くなる
    * 一部のファイルだけデタッチしている場合に、データが破損する可能性がある。
  * オフラインコンパクションの対象外にすべき
  * inspectについては、議論の余地がある。
    * 現在の使用だと、detached pwalは対象外にすべき
    * detached pwalの破損を調べられないのは、制限事項とする。
* inspectで表示される、persistent-format-version:が不正 => マニフェストファイルを見ていないのではないか
* コンパクションにより、エポックIDが0になることに起因する問題。
  * オンラインコンパクション直後に、リブートするとエポックIDが0から始まる。
  * 実際に実行してみたらそうならない
  * コンパクションカタログのエポックIDが0になることが問題
  * inspectコマンドの結果と、コンパクションカタログのepoch idが不一致になるパターンがある。
* ローテート済みのPWALとコンパクション済みのPWALがrepairの対象にならない
  * こららのファイルが破損したときに修復ができない
* 意図したファイルがローテーションされたかtsurugidbのログで確認できない。



## オペレーションのログ



   * tsurugidbを初期化して起動
   * バッチを実行
```
durable-epoch: 8696
max-appeared-epoch: 8696
count-durable-wal-entries: 1299932
MAX_EPOCH_ID 0
```
   * バッチを再実行
```
durable-epoch: 14902
max-appeared-epoch: 14902
count-durable-wal-entries: 2599362
MAX_EPOCH_ID 0
```
   * オンラインコンパクションを実行
```
durable-epoch: 14902
max-appeared-epoch: 14902
count-durable-wal-entries: 3500892  -> コンパクション済みのPWALとdetached PWALをダブルカウントしていると思われる
MAX_EPOCH_ID 14900 -> insepctコマンドの値と不一致
```
   * detached PWALを削除しバッチを実行
```
durable-epoch: 152867
max-appeared-epoch: 152867
count-durable-wal-entries: 2200960
```
   * オンラインコンパクションを実行
```
durable-epoch: 152867
max-appeared-epoch: 152867
count-durable-wal-entries: 2199954
MAX_EPOCH_ID 152865
```
   * tsurugidbを再起動後にバッチを＾実行
```
durable-epoch: 171794
max-appeared-epoch: 171794
count-durable-wal-entries: 3499886
```
* オンラインコンパクション
```
durable-epoch: 171794
max-appeared-epoch: 171794
count-durable-wal-entries: 4401416
```
* Tsurugiを初期化して起動、バッチを実行

```
durable-epoch: 5665
max-appeared-epoch: 5665
count-durable-wal-entries: 131201
MAX_EPOCH_ID 0
```
* ローテーション実行
```
durable-epoch: 5665
max-appeared-epoch: 5665
count-durable-wal-entries: 222731
MAX_EPOCH_ID 5665
```
* detached pwal削除

```
durable-epoch: 5665
max-appeared-epoch: 5665
count-durable-wal-entries: 91530
MAX_EPOCH_ID 5665
```

* tsurugi停止

```
durable-epoch: 5665
max-appeared-epoch: 5665
count-durable-wal-entries: 91530
MAX_EPOCH_ID 5665
```

* オフラインコンパクション実行

umegane@ubuntu22:~/git/phone-bill-benchmark/scripts$ !ls
ls -1 ~/tsurugi/tsurugi/var/data/log/
compaction_catalog
epoch
limestone-manifest.json
pwal_0000.compacted

* tusurgui起動、バッチ実行、tsurugi停止、オフラインコンパクション実行
```
durable-epoch: 7045
max-appeared-epoch: 7045
count-durable-wal-entries: 91530
MAX_EPOCH_ID 0
```







