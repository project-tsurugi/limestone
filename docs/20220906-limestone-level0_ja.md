# limestone level0 design document

2022-09-06 horikawa (NT)

## この文書について

tsurugiのdatastoreモジュール（limestone）level0の実装設計について記す。
なお、limestoneの実装は以下のドキュメントに準拠している。  
[1] arakawa, ログ機構のデザイン概要, 2022-01-17  
[2] arakawa, ロギングとデータストアのアーキテクチャ, 2022-01-20  
[3] arakawa, バックアップ・リストアの挙動について, 2022-03-04  
[4] arakawa, WP-2 以降のログ方式 (案), 2022-03-30  
[5] arakawa, データストアモジュールのI/Fデザイン, 2022-03-28

## terminology
* ログ:データベースにて行われたすべてのwrite操作を記録したデータ、ログを不揮発性の媒体に記録したものをログファイルと称する。
* ログ・エントリ：データベースにて行われた1回のwrite操作を表現するデータ。
* エポック（epoch）：複数のトランザクションの一括コミット（グループコミット）が完了する瞬間から次の一括コミットが完了する瞬間までの期間、
  あるエポックに属するログ（複数のログ・エントリ）はそのエポックが完了する時点にて生成される。
  各エポックには、単調増加する数値が1:1に対応しており、その数値をエポックIDと呼ぶ。
* スナップショット：特定時点（あるエポックが完了し、次のエポックが始まる直前）のデータベースイメージ。
* スナップショット・エントリ：データベースイメージを構成する分割不可能な最小単位。通常は、データベースの１レコードに対応する。
* データベースインスタンス：トランザクションの並行制御を行う単位、通常はトランザクションを実行するスレッドと1:1に対応する（同一の場合もある）。
* バックアップ：バックアップ操作を行った時点までのデータベース状態のリストアに必要な情報（データ）を作成して保存する操作。
* オンラインバックアップ：データベースを稼働させながら行うバックアップ操作・
* データストア：ログファイルの内容を再編し、sequential/random read を行いやすくした構造。
  データストアは必ずしも最新のコミット状態を反映しておらず、ログファイルと組み合わせて運用する。
* リカバリ：ログを再編し、データストアを最新の適切な状態へと変更する操作。
* リストア：適切な状態のデータストアから、適切な状態のデータベース状態を読みだしてデータベースに反映させる操作。

## 構成要素（オブジェクト）
本項では、limestoneの主要オブジェクトの役割を説明する。APIの詳細は[5]を参照のこと。

### datastore
役割：limestoneを使うモジュールから、limestone全体に関係する操作要求を受け付け、必要な処理を行う。  
作成：limestone外部のモジュール、トランザクション実行エンジン（shirakami）が作成することを想定している。

### log_channel
役割：トランザクション実行エンジンが作成したログ・エントリを受け取り、ログファイルに書き込む。  
作成：datastoreオブジェクトが、limestone外部のモジュールからの要求を受けて作成する。

### snapshot
役割：利用可能な最新のスナップショットを扱う。  
作成：datastoreオブジェクトが、limestone外部のモジュールからの要求を受けて作成する。

### cursor
役割：snapshotオブジェクトが扱うスナップショットに属するスナップショット・エントリへのアクセスを提供する。  
作成：snapshotオブジェクトが、そのsnapshotオブジェクトを取得したlimestone外部のモジュールからの要求を受けて作成する。

### backup
役割：バックアップ操作に必要な情報を、バックアップ操作を行うモジュールに提供する。  
作成：datastoreオブジェクトが、バックアップ操作を行うlimestone外部のモジュールからの要求を受けて作成する。

## behavior
### エポックID管理
datastoreオブジェクトは、データベースインスタンスが管理しているエポックIDの通知をdatastore::switch_epoch()により受け取る。
これにより、データベースインスタンスが書き込み要求したログエントリが属するエポックIDを特定できる。
また、log_channelオブジェクトと連携して「永続化に成功した最大のエポックID」を管理する。その情報は、datastore::last_epoch()により外部モジュールに提供する。

### データ投入（level0ではログ書き込み）
#### 前提
各データベースインスタンスは、稼働開始前にdatastore::create_channel()によりlog_channelを取得する。

#### ログ書き込み操作
あるエポックに属するログをデータベースインスタンスが書き込む際は、log_channelに対して
1. begin_session()を呼び出して、現在のエポックに属するログ・エントリの受け渡し開始を通知し、
2. 複数回のadd_entry()呼び出しにより書き込むべき全ログ・エントリを受け渡し、
3. end_session()を呼び出して、そのエポックに属するログ・エントリの受け渡し終了を通知する。

このとき、
* begin_session()からend_session()の間に、datastore::switch_epoch()が呼び出されても良い。
* データベースインスタンスを束ねるトランザクション実行エンジンは、あるエポックにおいて書き込むべきログを有する総てのデータベースインスタンスについてbegin_session()呼び出しから戻ったことを確認できれば、次のdatastore::switch_epoch()呼び出しが可能な状態となる。
すなわち、１つでもbegin_session()呼び出しから戻っていないデータベースインスタンスが残っている状態におけるdatastore::switch_epoch()呼び出しは禁止、という制約をlimestoneはトランザクション実行エンジンに課している。

また、あるエポックに属する総てのログ（～総てのデータベースインスタンスから書き込み要求されたログ）のログファイルへの書き込みを完了したら、そのエポックIDを「永続化に成功した最大のエポックID」に設定する。

現実装に関するnote）log_channel::end_session()は、begin_session()ごにadd_entry()で渡された全ログ・エントリのログ・ファイルへの書き込み完了後にリターンする。

### スナップショット読み出し（level0の実装）
#### 前提
スナップショットを必要としている外部モジュール（トランザクション実行エンジンを想定）は、limestoneにリカバリを要求（datastore::recover()）してスナップショットを作成し、
datastore::snapshot()またはdatastore::shared_snapshot()によりそのsnapshotオブジェクトへのポインタを受け取る。

#### スナップショット読み出し操作
1. snapshotオブジェクトに対してcursor()を呼び出してスナップショットを読み出すcursorを受け取り、
2. cursor->next()がfalseを返すまでloopし、スナップショットに存在する全スナップショット・エントリを読み出す。

### バックアップ
#### 前提
バックアップ操作を行う外部モジュールは、limestoneにバックアップ開始を通知（datastore::begin_backup()）してbackupオブジェクトを受け取る。

#### バックアップ操作
backupオブジェクトに記録されているログファイル等をバックアップ・ディレクトリにコピーする。

note)バックアップ・ディレクトリにコピーしたファイル一式をログディレクトリに書き戻してリカバリとリストア操作を行うことで、データベースはバックアップを取得した時点の内容となる。
