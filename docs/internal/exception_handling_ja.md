# Limestonenの例外の取り扱い

## 現状の問題

Limnestoneの処理の中で、I/Oエラーが発生すると、std::runtime_errorがスローされるが、
この例外が適切に処理されないことがある。

## 対応方針

* 致命的なIOエラー発生時には例外をなげる
* 例外はLimestone独自の例外を定義する
* 具体的には、limestone_exceptionと、そのサブクラスのlimestone_io_exception
* Limestoneは、外部に公開するAPIに対して、発生する可能性がある例外をドキュメント化する
  * 後述
* 呼び出し側がAPI仕様に合わせて、例外をハンドリグする。
* 呼び出し側の想定はshirakami/sharksfinと、ブートシーケンス

## limestone_exceptionをスローする可能性がある外部公開API

以下のメソッドは、limestone_exceptionをスローする可能性があり、
メソッドのコメントにlimestone_exceptionをスローする可能性がある旨を記載済み。

* cursor::next()
* snapshot::get_cursor()
* log_channel::begin_session()
* log_channel::end_session()
* log_channel::add_entry()
* log_channel::remove_entry()
* log_channel::add_storage()
* log_channel::remove_storage()
* log_channel::truncate_storage()
* datastore(configuration const& conf)
* datastore::ready()
* datastore::switch_epoch()
* datastore::begin_backup()
* datastore::compact_with_online()

※ 次のメソッドも、本質的にlimestone_excptioneをスローする可能性があるが、現在の実装ではスローしない。

 * datastore::shutdown()
 * datastore::restore_status()
 * datastore::switch_safe_snapshot()
 * datastore::recover()
 * snapshot::find()
 * snapshot::scan()
 * log_channel::abort_session()
 * datastore:restore_status()
 * datastore::switch_safe_snapshot()
 * datastore::recover()
 * datastore::shutdown()

