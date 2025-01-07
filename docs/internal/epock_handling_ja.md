# LimestoneのEpoch処理のまとめ

本ドキュメントは、現状(2024/12/03)のLimestoneの実装をもとに、
Epochに関する処理内容について記述したもので、以下の目的の
ために作成した。

* 他のモジュールとのAPI I/Fの整合性の確認
* 主に排他制御関連の処理の意図をドキュメント化する

対応するコミット: Commit 51a741d ([github](https://github.com/project-tsurugi/limestone/commit/51a741d076fffb2fc09ea8428bad5a84cb756037))

## LimestoneのEpoch処理の要件

### API

LimestoneのEpochに関連するAPIは以下の4つ(※1)。

1. datastore::switch_epoch(Epoch_id_type new_epoch_id)
  * 指定したEpochに切り替える。
2. log_channel::begin_session()
  * ログチャネルのセッションを開始する。
  * セッションのEpochは、直近のswitch_epoch()で指定されたEpoch。
3. log_channel::end_session()
  * ログチャネルのセッションを終了する。
  * ログ出力用のストリームをflush、syncしクローズする。
4. epoch_id_type datastore::last_epoch();
  * 最新の永続化済みのEpoch IDを返す。
  * 起動時にshirakamiが前回の終了時のEpoch IDを取得するために使用する。
  * shirakamiは起動時にこのAPIで取得した値より大きな値を最初のswitch_epoch()の引数に指定する。

※1 この他に、Persistance Callbackの登録のためのAPIがある。

### 呼び出しルール

上記1〜3のAPIメソッドは任意のタイミングで呼び出すことが可能だが、以下の制限事項がある。

* 同一のログチャネルに対する呼び出しのオーバーラップ
   * 同一のログチャネルに対するbegin_session()またはend_session()は、同時に1つのみ呼び出すことができる。
     * 同一のログチャネルに対して、複数のbegin_session()やend_session()が同時に呼び出された場合、動作は未定義。
* begin_session()とswitch_epoch()の呼び出しのオーバーラップ   
   * begin_session()とswitch_epoch()の呼び出しがオーバーラップした場合、セッションのEpochは次のどちらかの値になる。
    この挙動は、呼び出しのタイミングや実装の詳細に依存する。

     * switch_epoch()の呼び出し前の値
     * switch_epoch()で指定した値  
   * 例えば、switch_epoch()がEpochを10から11に切り替える途中でbegin_session()が呼び出された場合、
     セッションのEpochは10または11になる可能性がある。
   * 注意: API利用者は、どちらの値になっても正しく動作するように実装する必要がある。
     あるいは、begin_session()とswitch_epoch()の呼び出しをオーバーラップさせないようにAPI呼び出し側が制御する必要がある。
* switch_epoch()は複数同時に呼び出すことはできません。
   * 複数のswitch_epoch()が同時に呼び出された場合、動作は未定義。

switch_epochに指定するepoch_id前回指定された値より大きな値を指定する必要がある。
またラウンドアップすることは許されない。


### Epochの終了とEpochファイルの更新

Epochの終了とは、当該Epochに関わる全ての永続化操作(グループコミット)の完了を意味する。
ただし、当該Epochにおいて永続化操作が行われない場合、永続化操作が行われないことの確定
がEpochの終了となる。


以下の条件をすべて満たした場合、当該Epochが終了したとみなす。

* datastore::switch_epoch()により、当該Epochより新しいEpochに切り替えられた。
* 当該Epochと、当該Epochより小さな値のEpochを持つセッションが全て終了している。
  * 当該Epochを持つセッションが全て終了していても、それより小さなEpoch ID値を持つセッションが存在し得ることに注意。


#### Epochファイルの更新

Limestoneは終了したEpochを記録するために、Epoch終了時にEpochファイルに
終了したEpochのEpoch IDを書き込む。以降この操作をEpochファイルの更新と呼ぶ。

Limestoneは、Epochファイルの更新について以下の事項を保証する。

* Epoch ファイルへ書き込まれるEpoch IDが、前回書き込まれた値より大きいこと。
* Epoch ファイルへ書き込まれるEpoch IDがswitch_epoch()が最後に指定したEpoch IDより小さいこと。
* Epoch ファイルへ書き込まれるEpoch ID、およびそれより小さなEpoch IDを持つセッションが全て終了していること。
  * これにより、当該Epoch IDまでの全てのWALが永続化されていることが保証される。

複数のEpochが同時に終了するケースでは、小さい方のEpoch IDのEpochの更新はスキップされることがある。

#### Persistent Callback

Persistent Callbackは、特定のEpochが終了したことを通知するための仕組みである。

Persistent Callbackが登録されている場合、LimestoneはEpoch終了時に終了したEpochを
引数としてPersistent Callbackを呼び出す。

Limestoneは、Persistent Callback の呼び出しについて以下の事項を保証する。

* Persistent Callback が通知するEpoch IDが、前回通知したEpoch IDより大きいこと。
* 当該Epoch IDより大きな値でswitch_epoch()が呼ばれていること。
* 当該Epochで永続化操作が行われた場合、永続化操作が終了していること。

Limestoneは同時に複数のEpochが終了した場合、同時に終了したEpochのEpoch ID
の中で最大のEpoch IDをもつEpochを通知する。利用者は、通知されたEpoch ID以下の
Epochが全て終了しているとみなすことができる。

利用者は、switch_epochで指定したEpoch IDのEpochが終了したときに、必ずしも
当該Epoch IDのpersitent Callbackが呼ばれないことに対応する必要がある。
当該Epoch IDより大きなEpoch IDのEpochの通知をもって、当該Epochが終了したと
判断しなければならない。


**Note:**

**同時**とは、Limestoneの実装上の都合で発生する事項なので、厳密な定義は
行わない。


## Limestoneの実装

### 状態管理のためのAtomic変数

上記要件を満たすために、LimestoneはEpochに関する6種類のAtomic変数を使用している。

* datastoreオブジェクトのインスタンスフィールド
  * limestone全体で1つしか存在しない変数
  * 4種類のAtomic変数が存在する。

* log_channelオブジェクトのインスタンスフィールド
  * 各セッション毎に割り当てられるAtomic変数
  * 3種類のAtomic変数が存在する。ただし、そのうち1つは廃止予定である。

#### datastoreオブジェクトのインスタンスフィールド

* data_store::epoch_id_switched_
  * 最後のdatastore::switch_epoch()呼び出しで指定されたEpoch_idの値
  * limestoneの初期化時にlast_durable_epochがセットされる。
    * last_durable_epochは、Epochファイルの最後のEpoch_idの値
  * limestoneの初期化時に、完全にデータがない場合の初期値は0
* data_store::epoch_id_to_be_recorded_
  * 最後にEpochファイルに書き込んだEpoch_idの値
  * Epochファイルの更新前にこの変数を更新する。
  * 初期値は0
* data_store::epoch_id_record_finished_
  * 最後にEpochファイルに書き込んだEpoch_idの値
  * Epochファイルの更新後にこの変数を更新する。
  * 初期値は0
* datastore::Epoch_id_informed_
  * 最後にPersistent Callback に通知したEpoch_idの値
  * Persistent Callback の呼び出し前にこの変数を更新する。
  * 初期値は0

#### log_channelオブジェクトのインスタンスフィールド

* log_channel::current_epoch_id_
  * 当該ログチャネルがアクティブなときに、ログチャネルが書き込み中のセッションのEpoch IDを表す。
    ログチャネルがアクティブでない場合は、UINT64_MAXがセットされる。
  * セッション開始時に、epoch_id_switched_の値がセットされる。
  * セッション終了時に、UINT64_MAXがセットされる。
  * 初期値はUINT64_MAX
* log_channel::finished_epoch_id_
  * 当該ログチャネルが最後に終了したセッションのEpoch IDを表す。
  * セッション終了時に、current_epoch_id_の値がセットされる。
  * 初期値は0
* log_channel::latest_session_epoch_id_
  * セッション開始時に、epoch_id_switched_の値がセットされる。
  * current_epoch_id_と異なりセッションが終了しても保持される
  * コンパクション開始の判断のために使用しているが、不要なことがわかっているため廃止予定。=> Issue #1059
  * この変数は廃止予定なので、本ドキュメントの以降の記述では、この変数について言及しない。


**Note:**

* 「セッションがactiveかどうかの判断」の項参照

### LimestoneのEpoch処理の概要

本セクションでは、Epochに関するAPI呼び出し時に、どのような処理を行うかについて記述する。
Epochに関連しない処理については記述しない。正常系についてのみ記述し、競合が起きた場合の
動作については記述しない(後のセクションで記述)。

* log_channel::begin_session()
  * log_channel::current_epoch_id_に、datastore::epoch_id_switched_の値をセットする。

* log_channel::end_session()
  * 以下の順に処理する。処理順が重要。「セッションがactiveかどうかの判断」の項参照。
    * finished_epoch_id_に、current_epoch_id_の値をセットする。
    * datastore::update_min_epoch_id()を呼び出す。
    * log_channel::current_epoch_id_に、UINT64_MAXをセットする。

* datastore::switch_epoch(Epoch_id_type new_epoch_id)
  * 以下の順で処理する。処理順が重要。
    * epoch_id_switched_ に new_epoch_idをセットする。
    * datastore::update_min_epoch_id()を呼び出す。

* datastore::update_min_epoch_id()
  * upper_limitの決定
    * uppper_limitとは、durableになったEpochのEpoch IDの最大値を表す。
    * upper_limitは、Epochファイルに書き込むEpoch ID, Persistent Callback が通知するEpoch IDの候補となる。
    * アクティブなlog_channelが存在する場合、各ログチャネルのcurrent_epoch_id_の最小値 - 1
    * アクティブなlog_channelが存在しない場合、epoch_id_switched_ - 1

  * max_finished_epochの決定
    * 各ログチャネルのfinished_epoch_id_の最大値
  * Epochファイルの更新
    * to_be_epochの決定
      * switch_epoch()から呼ばれたとき
        * upper_limitとmax_finished_epochの小さい方の値をセットする
        * アクティブなセッションがない場合はmax_finished_epochがセットされる。
      * log_channel::end_session()から呼ばれたとき
        * to_be_epochにupper_limitをセットする。
    * epoch_id_to_be_recorded_より、to_be_epochが大きい場合、Epochファイルをto_be_epochの値で更新する。
      * 以下の順で処理する。処理順が重要。
        * Epoch_id_recorded_ にto_be_epochをセットする。
        * Epochファイルを更新する。
        * Epoch_id_recorded_ にto_be_epochをセットする。
  * Persistent Callback の呼び出し
    * Epoch_id_informed_より、upper_limitが大きい場合、Persistent Callback を呼び出す。

**Note:**

* 処理順とは、メモリ上への各変数への書き込み順序のことである。
* end_session()の呼び出しごとに、update_min_epoch_id()が呼び出される。
* 同一のEpoch IDを持つ最後のアクティブなセッションに対するend_session()の呼び出しでのみ、Epochファイルの更新や、Persistent Callback の呼び出しが行われる。
* アクティブなセッションが存在しない状態で、switch_epoch()が呼ばれた場合、
  Persistent Callback が呼び出されるが、Epochファイルの更新は行われない。
    * この場合、upper_limitにmax_finished_epochがセットされているが、通常はEpoch_id = max_finished_epoch
      で既にEpochファイルの更新が行われているため、Epochファイルの更新は行われない。
* 実際のソースコードでは、Epochファイルの更新処理に用いるto_be_epochと、Persistent Callbackの通知に用いる
  to_be_epochが存在するが、後者は常にupper_limitになるため、本ドキュメントでは論じない。本ドキュメントで、
  to_be_epochとしたものは、全てのEpochの更新処理に用いるものとする。

## 競合の排除

このセクションでは、どのようなレースコンディションを想定し、それを防ぐために、
どのような制御を行っているかを記述する。


### begin_session()と、switch_epoch()が同時に呼ばれたとき

[Issueのコメント](https://github.com/project-tsurugi/limestone/issues/63#issuecomment-2475492158)から転記


例として、switch_epoch()により、epoch_id_switched_の値が10から11に変わる処理と、
begin_session()が同時に呼ばれたときを考える。begin_session()により、当該セッション
のcurrent_epoch_id_が10または11のどちらかになるが、特定のタイミングで不整合が
発生する。

![Image](https://github.com/user-attachments/assets/601177ba-d636-4059-aa71-f8be78973d8b)

#### 発生しうる不整合

begin_session()のスレッドをL、switch_epoch()のスレッドをDとすると、以下のような処理順序で不整合が発生する。
ここでの「不整合」とは、Dによりepoch_id_switched_が11に更新された（つまり、10までdurableになったという
報告がshirakamiに送られた）後に、Lが10のセッションを開始すること。

1. L: epoch_id_switched_.load()  => 10
2. D: epoch_id_switched_.store(11)
3. D: current_epoch_id_.load() => UINT64_MAX
4. L: current_epoch_id_.store(10)

この順序で処理が行われたときに、Epoch ID=10のセッションがあるにも
かかわらず、スレッドDはそれを検知できずに、Epoch ID = 10 のPersistent Callback
を呼ぶ可能性がある。これはアクティブなセッションが存在し、当該エポックの永続化が
終了していない状態で、Persistent Callback が呼ばれる可能性があることを意味する。

#### 安全なケース

* 1と2が入れ替わった場合
  * Epoch ID=11のセッションが開始され、Epoch ID=10のPersistent Callbackが正しく呼び出される。
* 3と4が入れ替わった場合
  * スレッドDがEpoch ID=10のセッションを検知し、Epoch ID=10のPersistent Callbackを呼び出さない。


#### 対応方法

これを避けるため、次のコードで対応する。

```
do {
    current_epoch_id_.store(envelope_.epoch_id_switched_.load());
    std::atomic_thread_fence(std::memory_order_acq_rel);
} while (current_epoch_id_.load() != envelope_.epoch_id_switched_.load());
```

#### 仕組みの説明

* current_epoch_id_にepoch_id_switched_の値をセットし、その直後にメモリフェンス（std::atomic_thread_fence）を挿入することで、スレッド間の順序不整合を防ぐ。
* whileの条件がtrueになる場合、current_epoch_id_とepoch_id_switched_の値が一致するまで再試行する。このループによって、レースコンディションが解消される。
* epoch_id_switched_とcurrent_epoch_id_が LとDについて"read after write" の関係になっているので、「LとDのどちらか一方は必ず更新されたepoch ID（epoch_id_switched_ == 11 or current_epoch_id_ == 10）をreadする」という性質を利用する

### Epoch IDの更新順序の逆転

end_session()が2つ以上同時に呼ばれたケース、またはend_session()とswitch_epoch()が同時に呼ばれた
ケースでは、各スレッドが異なるEpoch IDでEpochファイルの更新やPersistent Callbackを呼び出す可能性が
ある。この場合、Epoch IDの昇順処理が保証されない可能性があるため、対策が必要である。

以下は、Limestoneで使用されるEpochファイル更新処理のコードである。

```cpp
    auto old_epoch_id = epoch_id_to_be_recorded_.load();
    while (true) {
        if (old_epoch_id >= to_be_epoch) {
            break;
        }
        if (epoch_id_to_be_recorded_.compare_exchange_strong(old_epoch_id, to_be_epoch)) {
            std::lock_guard<std::mutex> lock(mtx_epoch_file_);
            if (to_be_epoch < epoch_id_to_be_recorded_.load()) {
                break;
            }
            // ここにEpochファイルの更新処理が入る
            epoch_id_record_finished_.store(epoch_id_to_be_recorded_.load());
            break;
        }
    }
```

#### 昇順処理の保証

まず下のコードで、epoch_id_to_be_recorded_が昇順に更新されることを保証している。

```cpp
if (old_epoch_id >= to_be_epoch) {
    break;
}
if (epoch_id_to_be_recorded_.compare_exchange_strong(old_epoch_id, to_be_epoch)) {
```

* 2つのスレッドでold_epoch_id に異なる値がセットされるケース
  * to_be_epochが小さい方のスレッドが先に動けば、両方のスレッドでEpoch IDが更新される。
  * to_be_epochが大きい方のスレッドが先に動けば、小さいEpoch IDのスレッドはEpochファイルの更新処理をスキップされる。
* 2つのスレッドでold_epoch_id に同じ値がセットされるケース
  * 1つのスレッドが先にcompare_exchange_strong()に成功し、もう1つのスレッドは失敗する。
  * Epoch IDが小さい方のスレッドが失敗した場合、`old_epoch_id >= to_be_epoch`の条件でループを抜ける。
    * Epochファイルの更新処理をスキップされる。
  * Epoch IDが大きい方のスレッドが失敗した場合、ループにより再度compare_exchange_strong()を試みる。
    * こんどは、競合が発生しないのでcompare_exchange_strong()が成功し、大きいEpoch IDでEpochファイルの更新処理を行われる。


#### クリティカルセクション内での順序制御

以下の処理で、Epochファイルの更新処理が行われる。

```cpp
std::lock_guard<std::mutex> lock(mtx_epoch_file_);
if (to_be_epoch < epoch_id_to_be_recorded_.load()) {
    break;
}
// ここにEpochファイルの更新処理が入る
epoch_id_record_finished_.store(epoch_id_to_be_recorded_.load());
break;
```

* Epochファイルの更新処理は、std::lock_guardによるクリティカルセクション内で実行される。
* compare_exchange_strongの後、クリティカルセクションに入るまでの間に順序が逆転する可能性があるが、
  次のコードでその逆転を検知して処理をスキップする。

  ```cpp
  if (to_be_epoch < epoch_id_to_be_recorded_.load()) {
      break;
  }
  ```

* 小さいto_be_epochのスレッド:
  * この条件が真となり、ループを抜けて更新処理をスキップする。
* 大きいto_be_epochのスレッド:
  * この条件が偽となり、更新処理を実行する。

#### 補足

* この実装により、複数スレッドが同時にepoch_id_to_be_recorded_を更新しようとした場合でも、常に昇順が保証される。
* 更新処理がスキップされても、大きいEpoch IDでの更新が続行されるため、データの一貫性が保たれる。

### Persistent Callbackの呼び出し順序の逆転

Persistent Callbackの呼び出し順序についても、Epoch IDの更新順序と同様のロジックを用いて順序の逆転を防いでいる。
以下のコードがその実装である。

```cpp
old_epoch_id = Epoch_id_informed_.load();
while (true) {
    if (old_epoch_id >= to_be_epoch) {
        break;
    }
    if (Epoch_id_informed_.compare_exchange_strong(old_epoch_id, to_be_epoch)) {
        std::lock_guard<std::mutex> lock(mtx_epoch_persistent_callback_);
        if (to_be_epoch < Epoch_id_informed_.load()) {
            break;
        }
        if (persistent_callback_) {
            persistent_callback_(to_be_epoch);
        }
        break;
    }
}
```

### Persistent Callback Epoch ID条件の保証

#### upper limitと、to_be_epochの決定

以下のコードにより、upper_limitとto_be_epochが決定される。


```cpp
auto upper_limit = epoch_id_switched_.load();
if (upper_limit == 0) {
    return; // If epoch_id_switched_ is zero, it means no Epoch has been switched, so updating epoch_id_to_be_recorded_ and Epoch_id_informed_ is unnecessary.
}
upper_limit--;
Epoch_id_type max_finished_epoch = 0;
for (const auto& e : log_channels_) {
    auto working_epoch = e->current_epoch_id_.load();
    auto finished_epoch = e->finished_epoch_id_.load();
    if (working_epoch > finished_epoch && working_epoch != UINT64_MAX) {
        if ((working_epoch - 1) < upper_limit) {
            upper_limit = working_epoch - 1;
        }
    }
    if (max_finished_epoch < finished_epoch) {
        max_finished_epoch = finished_epoch;
    }
}

auto to_be_epoch = upper_limit;
if (from_switch_epoch && (to_be_epoch > static_cast<std::uint64_t>(max_finished_epoch))) {
    to_be_epoch = static_cast<std::uint64_t>(max_finished_epoch);
}
```

* アクティブなセッションが存在する場合、upper_limitは各セッションのcurrent_epoch_id_の最小値 - 1となる。
* アクティブなセッションが存在しない場合、upper_limitはepoch_id_switched_ - 1となる。


#### ケース1: アクティブなセッションが存在しない場合

* current_epoch_id_は、end_sessionの中で、update_min_epoch_id()が呼ばれた後に
UINT64_MAXがセットされる。UINT64_MAXのセッションはアクティブでないとみなされる。

  ```cpp
  finished_epoch_id_.store(current_epoch_id_.load());
  envelope_.update_min_epoch_id();
  current_epoch_id_.store(UINT64_MAX);
  ```

* アクティブなセッションが存在しない場合、epoch_id_switched_より小さいEpoch IDは、すでに更新済みであるか、更新する必要がないことが保証される。

* この場合、upper_limit = epoch_id_switched_ - 1がPersistent Callbackにより通知される。通知されるEpoch IDはPersistent Callback Epoch ID条件を満たしている。


#### ケース2: switch_epoch()以外から呼ばれた場合

* to_be_epochは常にupper_limitとなる。

* シングルスレッドでの動作を考えると、Epochファイルの更新後にPersistent Callbackが呼び出される。この順序により、Persistent Callback Epoch ID条件が満たされる。

* 他のスレッドとの競合の影響を考える。他のスレッドにより、Epochファイルがupper_limitより大きな値で更新される可能性があるが、このケースではPersistent Callback Epoch ID条件が満たされる。

* 次の条件が揃うと問題が起きる
  * 他のスレッドがこのスレッドのupper_limit以上の値でEpochファイルが更新中
  * 現在のdurable Epochがこのスレッドのupper_limitより小さい値
  * このスレッドのEpochファイルの更新処理がスキップされる
  * 他のスレッドのEPochファイルの更新処理の終了前に、このスレッドがPersistent Callbackが呼日出す。

  この場合、Epochの更新が終わっていないにもかかわらず、Persistent Callbackが呼ばれてしまう。

* これを避けるために、他のスレッドにより、Epochファイルがupper_limitより大きな値で更新済みであることを、次のコードで確認している。

  ```cpp
  if (to_be_epoch > epoch_id_record_finished_.load()) {
      return;
  }
  ```

* epoch_id_record_finished_はEpochファイル更新後に更新される。このため、durable Epochがepoch_id_record_finished_より大きいことが保証される。

* この条件が真の場合、Persistent Callback Epoch ID条件が満たされない可能性があるので、
Persistent Callbackの呼び出しはスキップされる。その場合でも、Epochファイルの更新中の
スレッドの方で、Persistent Callbackを呼び出すので、Persistent Callbackをスキップしても
問題ない。


#### ケース3: アクティブなセッションが存在し、switch_epoch()から呼ばれた場合


* 以下の条件により、to_be_epochはepoch_id_record_finished_以上の値となる。
  * upper_limitは常にepoch_id_to_be_recorded_以上である。  
    * アクティブなセッションのEpoch IDはepoch_id_to_be_recorded_より大きい値になるため。
  * epoch_id_to_be_recorded_は、epoch_id_record_finished_と等しいか、大きい値になる。
  * max_finished_epochはepoch_id_record_finished_以上の値となる。
     * epoch_id_record_finished_ が設定されたということは、Epoch IDがepoch_id_record_finished_のセッションが全て終了していることを意味する。
  * to_be_epochはupper_limitとmax_finished_epochの小さい方の値になる。
* 次のif文の条件は常に真になるため、Persistent Callback の呼び出しが行われない。

  ```
  if (to_be_epoch > epoch_id_record_finished_.load()) {
      return;
  }
  ```
* 常にPersistent Callbackが呼ばれないため、ケース3: では、Persistent Callback Epoch ID条件を考慮する必要がない。

### datastore::update_min_epoch_id()の同時呼び出しによる空振り

想定する競合

> 2つのスレッドが同時にend_session()からdatastore::update_min_epoch_id を呼び出した場合、
> 2つのスレッドが互いに相手のスレッドを呼び出したセッションがまだActiveと判断した結果、
> 本来呼ばれるべき、Epochファイルの更新も、Persistent Callbackの呼び出しも行われない。


2つのスレッドについて、互いのcurrent_epoch_id_が"read after write" の関係になっているので、
「両スレッドのどちらか一方は必ず更新されたcurrent_epoch_id_をreadする」という性質を利用
して競合を排除する。具体的には、以下のコードで対応する。


該当するコード。
```cpp
// log_channel::end_session()の該当箇所
finished_epoch_id_.store(current_epoch_id_.load());
envelope_.update_min_epoch_id();
current_epoch_id_.store(UINT64_MAX);
```

```cpp
// datastore::update_min_epoch_id()の該当箇所
for (const auto& e : log_channels_) {
    auto working_epoch = e->current_epoch_id_.load();
    auto finished_epoch = e->finished_epoch_id_.load();
    if (working_epoch > finished_epoch && working_epoch != UINT64_MAX) {
        if ((working_epoch - 1) < upper_limit) {
            upper_limit = working_epoch - 1;
        }
    }
    if (max_finished_epoch < finished_epoch) {
        max_finished_epoch = finished_epoch;
    }
}
```
当該エポックを処理中の最後の2つのセッションが同時にdatastore::update_min_epoch_idを呼び出したケースで
2つのスレッドで、working_epoch, finished_epochがどのような値を取るかを考える。

スレッドを呼び出したログチャネルの場合、working_epoch = finished_epoch = current_epoch_id_なので、
working_epoch > finished_epochが必ず偽になり、`upper_limit = working_epoch - 1;`は呼ばれない、

もう一方のスレッドを呼び出したログチャネルのworking_epoch、finished_epochの組み合わせは

1.  working_epoch = finished_epoch = current_epoch_id_ 
2.  working_epoch = current_epoch_id_, finished_epoch = current_epoch_id_ より小さい値
3.  working_epoch = UINT64_MAX, finished_epoch = current_epoch_id_

の3パターンが考えられる。1,3の場合、`upper_limit = working_epoch - 1;`が呼び出されない。
結果、upper_limitは、現在アクティブなセッションのEpoch -1 または、epoch_id_switched_ - 1
になり、エポックファイルが更新され、Persistent Callbackを呼び出す。

問題は、両方のスレッド2になるケースでupper_limitの値が Epoch_id_recorded_, Epoch_id_informed_ 以下の場合に
空振りする。

両方のスレッドが2になるケースは、どちらのスレッドもまだ、datastore::update_min_epoch_id()を呼び出していない
ケースだが、それはありえないので、両方のスレッドが2になることはなく、空振りしてエポックファイルが更新されなかったり、
Persistent Callbackを呼び出さないという現象は起きない。


## 競合以外のトピック

### Epoch IDのアンダーフロー

epoch_id_switched_が0の場合にupdate_min_epoch_id()が呼ばれると、Epoch_idに関連する演算でアンダーフローが
発生し意図しない結果が起き得る。これを避けるため、epoch_id_switched_が0の場合、update_min_epoch_id()は
なにも処理を行わないようにしている。

epoch_id_switched_が 0 の場合は、最小のEpoch IDの0のエポックがまだ終了していないので、
Epochファイルの更新や、Persistent Callbackの呼び出しは不要であるため、update_min_epoch_id()
の処理をスキップしても問題ない。

現実装では、shirakamiは、初期化時にepoch_id_type datastore::last_epoch()で、durable epochを取得し、
その値 + 1でepochを開始するので、epoch_id_switched_が0になることはないので、この処理がなくても
問題は起きない。


### セッションがaticveかどうかの判断

セッションがアクティブかどうかの判断は、current_epoch_id_がUINT64_MAXでないかどうかで行っているが、
finished_epoch_id < current_epoch_id_ で判断可能である。こうするとUINT64_MAXを特別な値として扱わなく
とも良くなる。

* 利点:
  * この方法を用いることで、UINT64_MAXを特別な値として扱う必要がなくなるため、コードが簡潔で読みやすくなる。
* 現状:
  * 現在の方法に特段の問題はないため、変更は行っていない。ただし、コードの改善余地として記録している。


