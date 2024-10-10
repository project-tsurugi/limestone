# 不要なソート処理の排除による起動時間の短縮

このドキュメントは、Limestone Log-0.7対応におけうｒ，不要なソート処理の排除による
起動時間の短縮の作業内容を記載しています。


## 概要

### 現行処理

* 起動時に全WALファイルを読み込み、ソートしてスナップショットファイルを作成している。
* 以下の2種類のWALファイルが存在する。
  * コンパクション済みファイル
    * ファイル名が`pwal_0000.compacted`
    * スナップショット作成と同じロジックで、コンパクション時に作成される。
    * 一度もコンパクションを行っていない場合、存在しない。
  * 通常のPWALファイル
    * ファイル名が`pwal_xxxx`
    * ローテーションが行われるとファイル名が、`pwal_xxxx.current_unix_epoch_in_millis`に変更される。
      * このPWALファイルのことを、ローテーション済みファイルと呼ぶ。
    * コンパクションカタログを参照し、コンパクション済みファイルとして記録されているPWALファイルは、
    除外される。
      * コンパクションは、ローテーション済みファイルに対して行われるので、除外されるのは必ず
      ローテーション済みファイルになる。
* スナップショットファイル
  * ファイル名は`snapshot`で固定。
* DB起動時にこのスナップショットにアクセスするためのカーソルを作成し先頭から順次読み込む。

※ この他に、コンパクション時に作成されるテンポラリファイルやバックアップファイル
  が存在するが、本ドキュメントの議論とは無関係なので省略する。


### 変更方針

* snapshot作成時の入力ファイルから、`pwal_0000.compacted`を除外する。
* スナップショット作成時のカーソルを`snapshot`ファイルだけでなく、`pwal_0000.compacted`にも
  対応するように変更する。
  * ファイルオープン時に、`pwal_0000.compacted`が存在する場合は、`pwal_0000.compacted`もオープンする。
  * カーソルが次の要素を読むときに、`snapshot`と`pwal_0000.compacted`の両方から次の要素を取得し、
  小さい方の値を採用する。
    * この通り実装すると、性能が悪化する可能性があるため、実際には論理的に同等で、もっと効率よく
    動作するようロジックを使用する。
* スナップショット/コンパクション済みファイルの作成に使用しているロジックは、最終的に削除されたエントリに対して、エントリ自体を削除してしまっている。
  *  スナップショット/コンパクション済みファイルが一つしか存在しない場合はこれで問題ないが、２つ存在する場合には問題が派生する。
     *  時系列順にファイルA，ファイルBの2つのコンパクションファイルが存在するケースで、特定のキーのエントリについて議論する。
     *  ファイルAにはエントリが存在する
     *  ファイルBのエントリは、Insert/Deleteの結果存在しない
     *  ファイルA, Bをマージした後、エントリは存在してはなｒないが、ファイルBに削除したという記録がなく、操作がまったく行われなかった場合と区別がつかないため、削除できない。
* 複数のスナップショット/コンパクション済みファイルの同一のキーに対して、特定のファイルには通常のエントリが存在し、別のファイルには削除エントリが存在する場合、当該エントリが存在するかどうｋは、バージョンの新しいエントリの値を取るべきだが、スナップショット/コンパクションファイル作成時に、バージョン情報を消している可能性がある。
  * このため、バージョンを使用できない。ファイルの新旧から、どちらのエントリを使用するのか判断する必要がある。  

* delete/truncateがどのように動作するのか把握できていないので、把握して処理する。

## 修正箇所

### datastore::ready()

* ここでスナップショットの入力となるファイルのセットを作成している。
* この処理で、`pwal_0000.compacted`を除外するように変更する。
  * 完了

### cusorクラス

* 現行では、コンストラクタにファイル名を1つだけ指定しているが、2つ指定できるようにする。
  * 完了
* next()メソッドで、2つのファイルから取り出したエントリのうち小さいほうのエントリを返すようにする。
  * 完了


### sortdb_foreach()

* 現状では削除エントリを無条件にスキップしているが、ファイル出力時にスキップするのか・出力するのかを指定できるようにする。
  * 完了


## TODO

* 入力ファイルのセットを作成する処理を、assemble_snapshot_input_filenamesに移動した。
  * `pwal_0000.compacted`を除外するよう変更は、現在コメントアウトしている。
    * 他の修正が終わらないと、テストが通らないため。 
    * テストが通るようになったら、コメントアウトを外す。=> 完了
    * assemble_snapshot_input_filenamesのテストがないので作成する。
* drop table, truncate table時に作成されるログエントリに対する処理が実装されてていない。
  * 現在の実装を確認し、それに合わせてロジックを組み込む必要がある。
  * pwal_0000.compacted => すでに存在するコンパクション済みファイル, snapshot => 起動時に作成するスナップショットファイル、pwal_0000.compactedのエントリを含まない。
  * pwal_0000.compacted にtruncate, drop tableのエントリが存在する場合
    * pwal_0000.compacted作成時に、truncate, drop tableの結果消えるエントリは、すでに削除済みなので、cursortアクセスで特に問題ない。
  * snapshot作成時に、truncate, drop tableのエントリが存在する場合
    * snapshot作成時に、消す必要があるエントリを削除しているので、snapshotに対するcursorのアクセスは特に問題ない。
    * pwal_0000.compacted にcurosrがアクセスする場合、truncate, drop tableにより削除すべエントリが存在する可能性があるので、それを発見したときにスキップする必要がある。
    * snapshot作成時に、削除したストレージのストレージIDのセットを作成しておき、cursorでpwal_0000.compactedアクセスじに、当該エントリのIDが見つかったらスキップする処理を追加する。
    * 


## テストケースの作成

どのコミットの変更に対するテストケース作成が終わったか

* 605844e Add test cases
  * このコミット自体がテストケースの追加
* c5b067f Refactor: Rename test file and class for compaction functionality
  * テストケースの修正のみなので必要なし
* 0146118 Add test cases
  * このコミット自体がテストケースの追加
* 8b8b8d6 Update an internal document
  * ドキュメントの追加のみなので必要なし
* b01068c (origin/wip/log-0.7) cleanup: clang-tidy
  * 新規にテストをつくる必要はなさそう
* aed365d Make cursor class handle two streams
  * cusor.cpp, cursor.hの変更 => 完了
* 68480e0 Add flag to snapshot creation to skip entry removal => 完了
* 1a48442 Refactor to isolate testable units for easier testing => 完了



## 2024/10/03

なんとか動き始めたので、性能改善度合いを測定してみる。

* TSURUGI_VERSION:snapshot-202409260127-4df7a55 を使ってテストデータを作る。
  * mediumデータを作成、バッチを3回流す

```
cd ~/git/phone-bill-benchmark/scripts/
git pull
./tinit.sh 
./multiple_execute.sh config-cb-medium/03-LTX-T16 config-cb-medium/03-LTX-T16 config-cb-medium/03-LTX-T16
./tstop.sh 
```
* データサイズ => 3273MB

```
cd  ~/tsurugi/tsurugi/var/data/
du -sm log
3273	log
```

* この状態でバックアップ

```
tar cf log.backup1.tar log
```

* 起動時間を測定 => 68.739s

```
cd ~/git/phone-bill-benchmark/scripts/
./tstart.sh 

real	1m8.739s
user	0m0.041s
sys	0m0.144s
```

* 停止後、コンパクションを実行 

```
./tstop.sh 
~/tsurugi/tsurugi/bin/tglogutil compaction ~/tsurugi/tsurugi/var/data/log --force
```

* データサイズ => 1516MB
```
cd  ~/tsurugi/tsurugi/var/data/
du -sm log
1516	log
```

* 起動時間を測定 => 26.703s
```
cd ~/git/phone-bill-benchmark/scripts/
./tstart.sh 

real	0m26.703s
user	0m0.022s
sys	0m0.021s
```

* 停止後、バックアップ

```
./tstop.sh 
cd ~/tsurugi/tsurugi/var/data/log
tar cf log.backup2.tar log
history | cut -c 8-
```

* 性能改善版に切り替え、コンパクション前のデータをリストア
```
cd ~/tsurugi/tsurugi/var/data/
cp log.backup*.tar ~/work
cd ~/tsurugi/
ln -snf tsurugi-snapshot-202410030200-2177fc0 tsurugi
cd ~/tsurugi//tsurugi/var/data/
rm -rf log
tar xf ~/work/log.backup1.tar 
```

* 起動時間を測定 => 68.796s
```
cd ~/git/phone-bill-benchmark/scripts/
./tstart.sh 

real	1m8.796s
user	0m0.043s
sys	0m0.122s
```

* コンパクション後のデータをリストア

```
cd ~/git/phone-bill-benchmark/scripts/
./tstop.sh 
cd ~/tsurugi//tsurugi/var/data/
rm -rf log
tar xf ~/work/log.backup2.tar 
```

* 起動時間を測定 => 14.807s
```
cd ~/git/phone-bill-benchmark/scripts/
./tstart.sh 

real	0m14.807s
user	0m0.010s
sys	0m0.016s
```
