# WALの安全性検討

本ドキュメントはIssue #1257で対応したWALの安全性に関する問題が
十分であるかを検討するために作成したドキュメントである。

このドキュメントでは、どのような破損があり得るかリストアップし、破損の検出と、
リペアが可能なこと、破滅的な状況にならないことを確認することを目的とする。

最初のステップでは、リストアップまでを実施し、テストデータやテスト
ケースを作成した検証の要否はリストアップの結果を見てから判断する。


## 前提事項

### 障害

本ドキュメントでは、単に障害と記述した場合、以下のいずれかを指す。

* Tsurugiプロセスのクラッシュ
* OSのクラッシュ
* サーバの電源断


### fopenなどのライブラリ関数について

fopen, fwrite, fsync, fflush, fcloseなどのライブラリ関数は、C/C++のライブラリ関数であり、
システムコールをラップしている。本ドキュメントでは、これらをシステムコールに準じるものとして、
単にシステムコールと呼ぶことがある。

### C++のI/Oライブラリについて

Limestone では、ファイル操作やディレクトリ操作において `std::filesystem` や `boost::filesystem` を利用している。

これらのライブラリ関数は、C++標準やBoostの抽象インターフェースを提供しているが、
内部的には Linux の `open`、`read`、`write`、`rename`、`unlink` などのシステムコールを呼び出している。

したがって、本ドキュメントで記述する各種システムコールの動作や障害時の挙動は、
これらのライブラリ経由で行われたファイル操作にもそのまま適用される。

そのため、`std::filesystem` や `boost::filesystem` の利用は、システムコールの議論にすでに含まれているものと見なす。

### リペアコマンド

WAL の検査および破損修復を行う `tglogutil repair` コマンドは、
本ドキュメントで想定している通常の WAL 書き込みパスとは異なる方法で
WAL ファイルにアクセスする。

このコマンドは、WAL に対して低レベルな修復処理を行うため、
リペア中に障害が発生した場合には、WAL ファイルが新たに破損する可能性がある。

ただし、`tglogutil repair` は運用上、リペア前に対象ファイルのバックアップを取得してから実行することを前提としており、
このリスクは運用手順で吸収されることを想定している。

したがって、本ドキュメントでは、リペアコマンド実行中に発生する障害やそれによる新たな破損は考慮対象外とする。

### O_SYNC

limestoneでは、O_SYNCフラグを指定した同期書き込みを行っていないため、
O_SYNCフラグを指定した場合の動作については本ドキュメントでは言及しない。


### WALの破損要因として考慮しない事項

#### 1. fsyncが期待通り動作しない

fsyncシステムコールが正常終了した場合、データはストレージに書き込まれたとみなす。
実際には、様々な要因により、fsyncが正常終了しても、データが失われる可能性があるが、これはシステム構築時や保守時に対応すべき問題と考え、本ドキュメントでは考慮しない。

考えられる要因を以下に列挙するが、これに限らない。

- ストレージの設定
- ハードウェア故障
- OSのバグ
- サーバ、およびストレージのFirmwareのバグ

#### 2. ファイルシステムのメタデータのジャーナリングの不備

ext4などのメタデータのジャーナリングを行うファイルシステムで、
メタデータのジャーナリングが有効であることを想定する。

ジャーナリングが有効でない場合、createやrenameなどのatomicな
操作を実現しているシステムコールでも、システムコール実行中の
電源断やOSのクラッシュでは、atomic性が保証されない。

ファイルシステムのメタデータのジャーナリングが無効であることに
起因する破損は本ドキュメントでは考慮しない。


#### 3. 確率的な要因で発生するデータ化け

宇宙線によるビット反転など、確率的な要因で発生するデータ化けは、
ソフトウェアでの対応が難しく、ECCなどのテクノロジーで対応するものとして考え、
本ドキュメントでは考慮しない。

ただし、WALにCRCをもたせ破損を検知すべきという議論もあるため、「後述の
その他の議論」として取り上げる。

#### 4. バグ


本ドキュメントは、OSとTsurugi仕様通り(設計意図通り)に動作した場合に、どのような
破損が起き得るかを議論することを目的とする。バグによるデータ破損は
本ドキュメントでは考慮しない。

#### 5. 操作ミス

操作ミスなど人為的な理由でファイルを破損させることは、本ドキュメントでは考慮しない。


## 想定するファイル破損

本セクションでは、システムコール実行中に障害が発生した場合に、
どのようにファイルが破損するかを議論する。

#### 1. creatシステムコール実行中の障害

次の可能性がある。

* ファイルが存在しない
* ファイルが存在するが、空である


limestoneはcreatシステムコールを直接呼び出していないが、2.のopen/fopenシステムコールを通じてファイルを作成するため、関連する破損の可能性は考慮する必要がある。

#### 2.open/fopenシステムコール実行中の障害

* ファイルが存在しない場合の動作は、1.に準じる。
* ファイルを追記モードでオープンする場合は、破損は起きない。
* WALは常に追記モードで書き込まれるので、上書きモードでの動作は議論しない。


#### 3 write/fwriteシステムコール実行中の障害

write/fwrite/fflush/fsync実行中の障害は、以下のような破損が起き得る。

本セクションで前回fsync成功時と記述している場合、open/fopen後にfsyncを
一度も実行していない場合のopen/fopen直後の状態を含む。

* 前回fsync実行時以降のデータが存在しない
* 前回fsync実行時のデータの末尾に0fillされたデータが追加される。
* 前回fsync実行時のデータに、完全なデータが追加される。
  * 完全なデータとは、fsync実行時に、それまでにwrite/fwriteで書き込まれた
    データがすべて含まれるデータを指す。
* 前回fsync実行時のデータに、部分的なデータが追加される。
  * 完全なデータの一部が欠損するケース
  * 多くの場合末尾が欠損するが
  * 先頭や中間のデータが欠損することもある。


次のケースは、現在のOSでは発生しないと考えられるが、POSIXの仕様上は発生しえる。

* 前回のfsync実行時のデータに、ランダムなデータを追加される。
* 前回のfsync実行時のデータに、意味のあるデータが追加される。
  * 過去に保存したデータなどが、そのまま追加されるケース。これは、重大なセキュリティホールとみなされる。本ドキュメントでは発生しないと考える。

#### 4. fcloseシステムコール実行中の障害

* 一般的には2.のwrite/fwriteシステムコール実行中の障害と同じ破損が起き得る。
* limestoneでは、WALの書き込み時にclose/fcloseの呼び出し前にfsyncを実行しているため、fclose実行中の障害によるファイルの破損は発生しない。


#### 5. rename, renameat2 システムコール実行中の障害

メタデータのジャーナリングが有効なファイルシステムでは、`rename` は
atomic な操作であるとされており、システムコール実行中に障害が発生した場合でも、
ファイルシステム上は次のいずれかの状態となる。

* rename 前の状態である
* rename 後の状態である

Limestone では、2 つのファイルのファイル名を入れ替える目的で、`renameat2`
システムコールを `RENAME_EXCHANGE` フラグ付きで利用している。
この操作も atomic に実行されるため、`renameat2` 実行中に障害が発生した場合でも、
次のいずれかの状態が保証される。

* rename 前の状態である
* rename 後の状態である

このため、いずれの場合でも整合性が失われることはなく、起動時のファイル検査により安全に再構築可能である。

---

#### 6. unlink システムコール実行中の障害

メタデータのジャーナリングが有効なファイルシステムでは、`unlink` も
atomic な操作であり、システムコール実行中に障害が発生した場合でも、
ファイルシステム上は次のいずれかの状態になる。

* unlink 前の状態である（ファイルが存在する）
* unlink 後の状態である（ファイルが存在しない）

Limestone では WAL ファイルの削除に `unlink` を使用しており、万一の障害によって
ファイルが削除されない場合でも、起動時に不要なファイルを検出して無視できる構成としている。
そのため、この種の障害が致命的な影響を与えることはない。




## WALの破損

### スニペット

本セクションでは、同一Epochのbegin_entryから始まる一連のWALエントリをまとめてスニペットと呼ぶ。
最新のTsurugiでは、スニペットはmarker_beginから始まり、marker_endで終わる。
旧バージョンのTsurugiでは、スニペットは、marker_beginから始まり、以下のいずれかで終了する。

* 次のmarker_beginが現れる
* EOFに達する

### スニペットの分類

障害によりWALが破損した場合、スニペットは以下のように分類される。

* 正常スニペット
  * marker_beginから始まり、スニペットを構成するlog_entryが全て正常なエントリ
* 完全欠落スニペット
  * スニペット全体が欠落している
* 先頭欠落スニペット
  * marker_beginのエントリの途中でEOFに達しているスニペット
* 先頭破損スニペット
  * marker_beginのエントリの途中ではEOFに達していないが、marker_beginのエントリのデータが不正なスニペット
    * このスニペットのepochは、偶然正しい値と一致しない限り不正な値になる。
* 破損スニペット
  * marker_beginから始まるが、marker_begin以外のエントリのデータが不正なスニペット
* 0fillスニペット
  * marker_beginから始まっていないので厳密にはスニペットではないが、便宜上破損したスニペットの一種として扱う。
* 先頭不正スニペット
  * marker_begin以外のエントリが先頭にあるスニペット
  * これも厳密にはスニペットではないが、便宜上破損したスニペットの一種として扱う。
  * 0fillスニペットも先頭不正スニペットであるが、本ドキュメントでは別扱いとする。


### 想定される破損

前セクションでの議論から、WALの破損は以下のようなものが考えられる。

* リネームしたWALファイルの一部(または全て)がリネームされていない
* 削除しようとしたWALファイルが削除されていない
* 作成したWALファイルが存在しない
* 作成したWALが正常スニペットを持たない
* 最後のスニペットが破損している。
  * 完全欠落スニペット
  * 先頭欠落スニペット
  * 先頭破損スニペット
  * 破損スニペット
  * 0fillスニペット
  * 先頭不正スニペット


### 想定される破損がlimestoneでどう扱われるか

#### リネームしたWALファイルの一部(または全て)がリネームされていない

* WALのローテーション中に障害が発生した場合に発生する。
* limestoneはローテーションは起動時に、ローテーション前後のWALを同様に扱うため、ローテーションによるファイル名の変更の有無により、起動時の動作には影響しないため、問題とならない。
* ローテーションは、バックアップおよびコンパクションに先立って行われる。
  * ローテーション中に障害が発生した場合、バックアップが失敗する以上の影響はない。
  * コンパクションは、ローテーション済みかつ、コンパクションしていないWALをコンパクションの対象にするので、ローテーション中に障害が発生した時、ローテーション済みのファイルはコンパクションの対象となり、未ローテーション(ファイルがリネームされていない)WALは、コンパクション前にローテーションの対象となるため、問題ない。

#### 削除しようとしたWALファイルが削除されていない

以下により問題ない。

* Tsurugi起動中のWALの削除は、コンパクション後に不要となったWALファイルを削除する際に実行される。
* limestoneは削除対象のWALを保持し、実際にファイルを削除した後で、削除対象リストからそのWALファイルを除去する。
* 削除中の障害により削除完了しない場合、このリストにWALファイルが残るため次の削除で削除対象となる。


#### 作成したWALファイルが存在しない

完全欠落スニペットと同じ

#### 作成したWALが正常スニペットを持たない

最後のスニペットが破損している場合と同じ。

詳細は、以下の破損スニペットのセクションを参照

* 先頭欠落スニペット
* 先頭破損スニペット
* 破損スニペット
* 0fillスニペット
* 先頭不正スニペット

#### 完全欠落スニペット

* limestoneはWALのfsync終了後epochを更新し、更新したepoch以下のepochをdurable epochとして扱う。
* このため完全欠落スニペットは、必ずnon-durable epochとなる。
* non-durable epochのスニペットは、WALに存在しないのが正常である。
* したがって、完全欠落スニペットは常に正常である。

#### 先頭欠落スニペット

* marker_endが導入されている場合完全欠落スニペットは、必ずnon-durable epochであるとみなすことができる。
* marker_endが導入されている場合、先頭欠落スニペットは常にリペア可能な破損とみなされる。

* marker_endが導入されていない場合、先頭欠落スニペットは、先行するスニペットの破損と区別がつかない。
* marker_endが導入されていない場合、先頭欠落スニペットはリペア不可能な破損とみなされる。
  * WALが先頭欠落スニペットから始まる場合、リペア可能な破損とみなされるべき(実装がどうなっているかは要調査)


#### 先頭破損スニペット

* 先頭破損スニペットは、marker_beginの値が不正であるが、正常なエントリとして処理される。
* marker_beginのepochがdurable epochである場合、
  * 後続のepochが偶然全て正しいentryである場合、DBの値が不正になる可能性がある。ただし、この可能性は非常に低く、ほぼ起きないとみなせる。marker_endにCRCを導入すれば、さらに低くなる。
  * ほとんどのケースでは、後続のentryが不正であることから、当該スニペットは修復不能な破損とみなされる。
* 先頭破損スニペットのmarker_beginのepochがnon-durable epochである場合、先頭破損スニペットは常にリペア可能な破損とみなされる。
* 先頭破損スニペットは、marker_endの導入の有無に関わらず同様に処理される。


#### 破損スニペット

* limestoneはWALのfsync終了後epochを更新し、更新したepoch以下のepochをdurable epochとして扱う。
* このため破損スニペットは、必ずnon-durable epochとなる。
* 破損スニペットは、marker_endの導入の有無に関わらず、修復可能な破損とみなされる。

#### 0fillスニペット

* marker_endが導入されている場合0fillスニペットは、必ずnon-durable epochであるとみなすことができる。
* marker_endが導入されている場合、0fillスニペットは常にリペア可能な破損とみなされる。

* marker_endが導入されていない場合、0fillスニペットは、先行するスニペットの破損と区別がつかない。
* marker_endが導入されていない場合、0fillスニペットはリペア不可能な破損とみなされる。
  * WALが0fillスニペットから始まる場合、リペア可能な破損とみなされるべき(実装がどうなっているかは要調査)
 
#### 先頭不正スニペット

* 先頭不正スニペットは、marker_begin 以外のエントリが先頭に現れるスニペットであり、構造上 marker_begin から始まっていないため、厳密にはスニペットとはみなされないが、便宜上破損スニペットの一種として扱われる。
* 内容的には 0fillスニペット と同様であり、判定・修復の処理も同一である。
* marker_end が導入されている場合、当該スニペットは必ず non-durable epoch に属するため、リペア可能な破損とみなされる。
* marker_end が導入されていない場合、先行するスニペットとの境界が不明確であるため、リペア不可能な破損とみなされる。
* ただし、WALファイルがこのスニペットから始まる場合には、リペア可能な破損とみなされるべきである（実装における扱いは要確認）。


## 修復不可能な破損について

### 先頭破損スニペット

* marker_beginが存在するが、epochが不正な値である場合
* 障害時にWALにランダムな値が追加され、その先頭が0x01で始まる場合に相当する。
* 不正なepochがnon-durable epochである場合、リペア可能な破損とみなされる。
* 不正なepochがdurable epochである場合、リペア不可能な破損とみなされる。
  * ほぼありえないと考えるが、不正なepochがdurable epochで、marker begin以降のエントリが全て正常なエントリである場合、DBの値が不正になる可能性がある。
  * この可能性は非常に低く、ほぼ起きないとみなせる。
  * marker_endにCRCを記録し、チェックするようにすれば、さらに低くなる。

### marker_endの導入により起きなくなった破損

* 先頭欠落スニペット
* 0fillスニペット
* 先頭不正スニペット

これらのスニペットは、直前のスニペットにmarker_endが存在しないと、
修復不能な破損とみなされる。

WALがこのスニペットから始まる場合、リペア可能な破損とみなされるべきである（実装における扱いは要確認）。

旧フォーマットのWALを読んだあと、最初のWALの書き込みでこれらのスニペットが発生した場合、
修復不能な破損とみなされるので対応した方が良い。

起動時にmarker_endが書かれていないWALを検出したら末尾にmarker_endを追加するようにすれば、良さそう。

### CRCの導入

CRCを導入しないと破滅的な事態がおきるケースはみつかなかったが、確率的に発生するデータ化けに
対応する意味でもCRCを導入を検討すべき。

CRCを導入しない場合でも、marker_endにCRCの有無とCRC値を格納可能にし、CRC導入時に
ファイルフォーマット変更が発生しないようにするという案もある。


