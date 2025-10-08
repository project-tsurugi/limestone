
# 20251008-persistent-callback-design.md
2025-10-08 umegane(NT)
## 1. 概要

本ドキュメントは、Limestone における **persistent callback** の新仕様を定義する。
本機能は、トランザクションコミットの進行状態およびレプリケーションモードの変化を、
上位コンポーネント（Shirakami、Jogasaki）へ通知するためのインターフェースである。




## 2. 目的

従来の persistent callback は「永続化成功」イベントのみを通知していた。
新仕様では、コミットの進行状態（ローカル永続化、レプリケーション成功、失敗確定）と、
クラスター全体のレプリケーションモード（正常、縮退、閉塞）を組み合わせた
イベント通知を提供する。


## 3. レプリカ数と動作モード


### 3.1 レプリカ数

- **レプリカ数**: クラスター内で稼働中のデータベースインスタンス（Master含む）の合計数。レプリケーションを行わない場合、レプリカ数は1。

- **総レプリカ数**: クラスター全体で管理されるレプリカの総数。確定数・維持数の上限となる。
- **確定数**（コミット確定レプリカ数）: レプリケーションコミットを確定させるために十分なレプリカ数。確定数以上のレプリカにログ転送が成功した場合、コミットは正常とみなす。
- **維持数**（システム維持レプリカ数）: システムの継続に必要な最低限のレプリカ数。維持数未満の場合、システムは運転を停止する。

各閾値は以下の関係を満たす：

```
    1 <= 維持数 <= 確定数 <= 総レプリカ数
```

### 3.2 動作モード

- **正常モード**: 残存レプリカ数が確定数以上の場合。

- **縮退モード**: 残存レプリカ数が確定数未満だが維持数以上の場合。応答性能が低下し、データ損失リスクがあるものの、運転は継続される。

- **スタンドアローンモード**: レプリケーションを行わず単独動作している状態。

- **閉塞モード**: 残存レプリカ数が維持数未満の場合。新規トランザクションは開始できず、システムは閉塞状態となる。

レプリケーションに失敗した場合、該当レプリカは切り離され、それ以降そのレプリカへのレプリケーションは行われない。
レプリカ数は減少し、維持数・確定数の閾値に応じて動作モードが変更される。



## 4. 仕様概要



### 4.1 通知タイミング

Persistent callback は、以下の 3 種類のイベントで発火する。

| 状況 | commit_status | 説明 |
|------|----------------|------|
| ローカルコミット成功 | `stored` | エポック単位のローカル永続化が完了した時点 |
| レプリケーションコミット成功 | `propagated` | 規定数のレプリカに対してレプリケーションが完了した時点 |
| コミット失敗確定 | `failed` | レプリカ数が維持数未満となり、コミットが失敗と確定した時点 |




### 4.2 レプリケーションモード


各イベントは、発火時点における Limestone 全体のレプリケーションモードを示す。
これは個々のエポックに依存しないクラスター全体のスナップショットである。

| モード            | 説明 |
|-------------------|------|
| `normal`          | 正常モード：確定数以上のレプリカが存在し、通常運転中 |
| `degraded`        | 縮退モード：確定数未満だが維持数以上のレプリカで運転中 |
| `standalone`      | スタンドアローンモード：レプリケーションを行わず単独動作 |
| `blocked`         | 閉塞モード：維持数未満でlimestoneが閉塞状態 |




### 4.3 コールバックデータ型定義

```cpp
namespace limestone::api {

enum class commit_status {
    stored,
    propagated,
    failed
};

enum class replication_state {
    normal,        ///< 正常モード
    degraded,      ///< 縮退モード
    standalone,    ///< スタンドアローンモード
    blocked        ///< 閉塞モード
};


struct persistent_event {
    epoch_id_type epoch_id;
    commit_status status;
    replication_state repl_state;
    std::string message;
};


} // namespace limestone::api
```



### 4.4 登録 API

#### 4.4.1 現行API

```cpp
/**
 * @brief register a callback on successful persistence
 * @param callback a pointer to the callback function
 * @attention this function should be called before the ready() is called.
 */
void add_persistent_callback(std::function<void(epoch_id_type)> callback) noexcept;
```

#### 4.4.2 新API（本仕様）

```cpp
/**
 * @brief register a persistent callback with replication mode and commit status
 * @param callback a pointer to the callback function
 * @attention this function should be called before the ready() is called.
 */
void set_persistent_callback(std::function<void(persistent_event)> callback) noexcept;
```



### 4.5 コールバック呼び出し規約

1. **同期呼び出し**
   - Limestone 内部処理スレッドから直接呼び出す。
   - 呼び出し先は制御を即時返すこと。
2. **例外非伝搬**
   - コールバック実装は例外をスローしてはならない。
3. **呼び出し順序保証**
    - 同一のepoch_idに対して複数の通知が行われる場合、その順序は保証されない。
    - epoch_idの順序は保証される。
4. **重複通知**
   - 同一のepoch_id、commit_statusの通知は一回のみ発生することを保証する。
   - 同一のepoch_idに対して、異なるcommit_statusの通知は複数回発生する可能性がある。

### 4.6 コミット成功判定方法


コミットの完全な成功・失敗は、`commit_status` と `replication_state` の組み合わせで次のように判定する。

#### 1. スタンドアローンモード（replication_state = standalone）
- `stored` イベントが通知されたら、そのepochのコミットは成功
- `failed` イベントが通知されたら、そのepochのコミットは失敗

#### 2. 正常・縮退モード（replication_state = normal または degraded）
- `stored` および `propagated` イベントの両方が通知される。
- 両方の通知が発生した時点で、そのepochのコミットは完全に成功と判定する。
- どちらか一方しか通知されていない場合は、まだ失敗の可能性がある。
- `failed` イベントが通知された場合は、そのepochのコミットは失敗と判定する。

#### 例
- 例1: `standalone` + `stored` → 成功
- 例2: `normal` + `stored` → まだ未確定
- 例3: `normal` + `stored` + `propagated` → 完全成功
- 例4: `degraded` + `failed` → 失敗
- 例5: `normal` + `propagated` + `failed` → 失敗


### 4.7 永続化が行われないEpochの動作

旧仕様では当該epochの永続化が行われない場合でもpersistent callbackが呼ばれていたが、本対応以降はこの動作はなくなる


## 5 議論

### 5.1 4.6のコミット成功判定について

- 現状の4.6のコミット成功判定は、commit_statusとreplication_stateの組み合わせで判断するため、やや複雑なロジックとなっている。
- commit_statusに `completed` を追加すれば、利用者が「完全なコミット成功」をより直感的に判定できる可能性がある。
- さらに、commit_status自体を次のような段階的状態にする案も考えられる：
    - committing
    - stored_and_not_propagated
    - propagated_and_not_stored
    - completed
    - failed
- どの設計が最適かは、コールバック利用側がどのように使うのが一番分かりやすいか、実際のユースケースに基づいて決定すべき。

### 5.2 4.7の変更による影響について

- 4.7の「永続化が行われないEpochではpersistent callbackを呼ばない」仕様変更によって、
    既存の上位モジュールの処理に影響が出ないか十分な確認が必要。

### エラーメッセージ

- エラーメッセージ用に`message`フィールドを追加したが、現状では、ここに何を格納するのか未定である。