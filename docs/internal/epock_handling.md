# LimestoneのEpoch処理のまとめ


## LimestoneのEpoch処理の要件

* Epochに関するLimestoneのAPIは以下の3つ(※1)
  * datastore::switch_epoch(epoch_id_type new_epoch_id)
    * 指定のEpochに切り替える。
  * log_channel::begin_session()
    * ログチャネルのセッションを開始する。
    * 当該セッションのEpochは、直近のswitch_epoch()で指定されたEpochになる。
  * log_channel::end_session()
    * ログチャネルのセッションを終了する。
    * ログ出力用のストリームをflush, syncしクローズする。

* 上記3メソッドは任意のタイミングで呼び出される。呼び出しがオーバーラップすることもありえる。
  * ただし、同一のログチャネルのbegin_session()とend_session()はオーバラップすることはない。
  
* limestoneはEpoch終了時に、Epochファイルの更新と、persistent_callback_の呼び出しを行う。
  * 次の条件が満たされるとき、当該Epochが終了したとみなす。
    * datastore::switch_epoch()により、当該Epochより新しいEpochに切り替えられた。
    * 当該Epochのセッションが全て終了した。

※1 この他に、persistent_callbackに関するAPIがある。

## LimestoneのEpoch処理の概要

### Epochの状態を管理する変数

以下のatomic変数で、Epochの状態を管理している。各atomic変数の参照、更新時に
mutxなどによる排他制御や同期は行われていない。

* data_store::epoch_id_switched_
  * 最後のdatastore::switch_epoch()呼び出しの引数で指定されたepoch_idの値
* data_sotre::epoch_id_recorded_
  * 最後にepochファイルに書き込んだepoch_idの値
* data_store::epoch_id_informed_
  * persistent_callback_で通知したepoch_idの値
* log_channel::current_epoch_id_
  * セッション開始時に、epoch_id_switched_の値がセットされる。
  * セッション修了時にクリアされ、UINT64_MAXがセットされる。
* log_channel::finished_epoch_id_
  * セッション修了時に、current_epoch_id_の値がセットされる。

※ この他に、コンパクション処理で以下の変数を使用しているが、本ドキュメントの議論の範囲では使用していないので、本ドキュメントのスコープ外とする。

* log_channel::latest_session_epoch_id_
  * セッション開始時に、epoch_id_switched_の値がセットされる。
  * current_epoch_id_と異なりセッションが終了しても保持される

### datastore::update_min_epoch_id()の処理内容

datastore::switch_epoch()呼び出し時と、log_channel::end_session()呼び出し時に、datastore::update_min_epoch_id()が呼び出される。この中で必要に応じで、epochファイルの更新と、persistent_callback_の呼び出しが行われる。

#### upper_limit の決定

* アクティブなlog_channelが1つもない場合
  * epoch_id_switched_ - 1 
* アクティブなlog_channelがある場合
  * アクティブなlog_channelの中で最小のcurrent_epoch_id_ - 1
* epochがこの値より大きくなってはいけないという意図だと思う。
  
#### max_finished_epochの決定

* 各log_channelの中で最大のfinished_epoch_id_をmax_finished_epochに設定する。

#### epochファイルの更新

* to_be_epochの決定
  * switch_epochから呼ばれたとき
    * uppper_limtとmax_finished_epochの小さい方の値をセットする
    * アクティブなセッションがない場合はuppper_limtがUINT64_MAX - 1なので、max_finished_epochがセットされる。
  * log_channel::end_session()から呼ばれたとき
    * uppper_limtにto_be_epochをセットする。
* epoch_id_recorded_がto_be_epoch以上の場合、epochファイルの更新は行わない。そうでない場合は、epoch_id_recorded_ を更新し to_be_epoch の値をセットする。
  * この処理は、epoch_id_recorded_.compare_exchange_strong()を使用して行う。
  * 他のスレッドによりepoch_id_recorded_が更新された場合は、他のスレッドにより更新された値とto_be_epochの値を比較し、to_be_epochの方が小さい場合、epochファイルの更新処理を行わない。
* epochファイルの更新
  * epoch_id_recorded_の値をepochファイルに書き込む。

#### persistent_callback_ の呼び出し

* to_be_epochの決定
  * uppper_limtをto_be_epochに設定する。
* epoch_id_informed_がto_be_epoch以上の場合、persistent_callback_の呼び出しは行わない。そうでない場合は、epoch_id_informed_を更新し、to_be_epochの値をセットする。
  * この処理は、epoch_id_informed_.compare_exchange_strong()を使用して行う。
  * 他のスレッドにより、epoch_id_informed_が更新された場合は、他のスレッドにより更新された値とto_be_epochの値を比較し、to_be_epochの方が小さい場合、persistent_callback_の呼び出しを行わない。

