# ロケーションプレフィックス自動算出

実装: src/limestone/logging_helper.h

参考: ロケーションプレフィックスについて
https://github.com/project-tsurugi/jogasaki/blob/master/docs/logging_policy.md#%E3%83%AD%E3%82%B1%E3%83%BC%E3%82%B7%E3%83%A7%E3%83%B3%E3%83%97%E3%83%AC%E3%83%95%E3%82%A3%E3%83%83%E3%82%AF%E3%82%B9

## 機能

`__PRETTY_FUNCTION__` から C++ のクラス名や namespace などを抜き出し、ロケーションプレフィックスを自動算出する。
コンパイル時定数 (constexpr) として計算される。

## 変換規則

* テンプレート変数 `<...>` → 取り除く
* テンプレート展開内容 `[T = ...]` → 取り除く
* lambda → lambda を宣言した位置の location_prefix の末尾に `:lambda` を追加したもの
    * namespace なしの global 空間に作られた lambda は `lambda`
* 演算子オーバーロード `operatorXX` → すべて `operator` という名の関数扱い。演算子の記号は取り除く
    * ロケーションプレフィックスには、その定義上、記号を入れられないため
* キャスト `operator type ()` → すべて `cast` という名の関数扱い。変換先の型情報は取り除く
    * cast_bool などとはしない
        * 型に `:` や ` ` が入ることがある
        * 処理系によって型名が展開されたりされなかったりする。`operator size_t ()` が g++ では `operator size_t()` なのに対し、 clang++ では `operator unsigned long()` となる

## 制約事項

* `__PRETTY_FUNCTION__` は C, C++ の規格に存在しないため、使用できる処理系は限られる
    * GCC, CLANG 系列は使用できる
* (ver.2) `__PRETTY_FUNCTION__` 文字列は ASCII を仮定している
    * 文字種チェックで 0-9, A-Z, a-z が並んでいると仮定している

## 変更履歴

### Ver.2

* v2 はある程度複雑な文字列解析をする
    * そうでないと `operator<` のような、括弧が関数名に入るようなものに対応できない
    * 未知の形式の `__PRETTY_FUNCTION__` に遭遇するとコンパイルエラーになる
* (Debian 11 の g++-9, g++-10 である) g++ 9.3, 10.2 には constexpr の処理にバグがありコンパイルできない
    * 参考: [GCC #98672](https://gcc.gnu.org/bugzilla/show_bug.cgi?id=98672)

### Ver.1

* v1 は比較的単純な文字列検索と置換のみを実施しており、 `__PRETTY_FUNCTION__` の解析に失敗した場合には  `__FUNCTION__` にフォールバックする
* lambda, 演算子オーバーロード, キャストに対応していない
    * `__FUNCTION__` にフォールバックした挙句、ロケーションプレフィックスに含めてはならない記号文字が入る
