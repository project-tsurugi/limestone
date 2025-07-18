# parse_wal_file.cppの分析

## この文書について

この文書は[Issue #1257](https://github.com/project-tsurugi/tsurugi-issues/issues/1257)対応の
ためにWALのパースとリペア処理を行っているコードの分析をしたときのメモです。

分析結果を情報として残しておくことを目的に保存しますが、
内容の正確性の保証はありません。また、ソフトウェアの更新に追従することもありません。

## chatgptによる分析

### 何をしているかの概要

- `scan_one_pwal_file` は pWAL (persistent Write Ahead Log) ファイルを先頭から順に読み取り、エポック単位のスニペットを解析する。
- 各スニペットの先頭は `marker_begin` または `marker_invalidated_begin` で始まる。
- `marker_begin` で始まったスニペットが **durable epoch** 以内なら有効、超えていれば無効化またはエラー扱いとする。
- 中の `normal_entry` などは有効なら集計関数 `add_entry` に渡される。
- 読み取り中に破損が見つかれば、設定ポリシーに従ってマーク付け・切り詰め・報告を行う。
- 最終的に、ファイル内で見つかった最大のエポック番号を返す。

---

### 重要な状態

| 変数                | 説明 |
|---------------------|------|
| `valid`             | 現在読み取り中のスニペットが有効か |
| `marked_before_scan` | スニペットがすでに無効化されていたか |
| `invalidated_wrote` | スキャン中に無効化したかどうか |

---

### トークン分類 (`lex_token`)

`log_entry` を読み取った結果を `lex_token` により分類し、状態機械で分岐する。

| トークン                   | 意味 |
|----------------------------|------|
| `marker_begin`             | スニペット開始 |
| `marker_invalidated_begin` | 無効化済みスニペット開始 |
| `normal_entry` 系          | 通常のログ |
| `SHORT_*`                  | 不完全（途中切れ） |
| `UNKNOWN_TYPE_entry`       | 型不明 |
| `eof`                      | ファイル終端 |

---

### エラーとして扱う主なパターン

| ケース | 詳細 |
|--------|------|
| 1 `normal_entry` 系が1個目に出る | スニペット開始マーカがない → `parse_error::unexpected` |
| 2 durable epoch を超える `marker_begin` | `process_at_nondurable` に従って無効化/エラー |
| 3 `SHORT_*` | 途中で切れている → `process_at_truncated` に従う |
| 4 `SHORT_marker_begin` | 開始マーカ自体が不完全 → `process_at_truncated` に従う |
| 5 `SHORT_marker_inv_begin` | 無効化済みスニペットでの途中切れ → 軽度だが `process_at_truncated` に従う |
| 6 `UNKNOWN_TYPE_entry` | 不明な型 → `process_at_damaged` に従う |

---

### 成功パス

- `marker_begin` でスニペットを開始。
- 有効なら `normal_entry` 系を `add_entry` に渡す。
- `marker_invalidated_begin` は無効としてスキップ。
- 最後に `eof` で正常終了。

---

### ファイル切り詰め

`process_at_truncated` または `process_at_damaged` が `repair_by_cut` の場合、
該当位置でファイルを物理的に `resize_file` で切り詰める。

---

### まとめ

| 状態 | どう扱うか |
|------|------------|
| `marker_begin` 以外で開始 | 不正、unexpected |
| durable超えエポック | ポリシーに従う |
| 不完全エントリ | ポリシーに従う |
| 不明型 | ポリシーに従う |
| 正常EOF | 正常終了 |

---

### `process_at_nondurable` の決まり方

- `process_at_nondurable` はスキャンポリシーで指定される。
- コンストラクタの引数や設定ファイルで外部から与えられる。
- 例: `ignore` / `repair_by_mark` / `report` を設定できる。
- デフォルト値がある場合もある。

---

必要に応じて DFA 図や構造体の定義などもドキュメントに含めると理解がより深まる。

## いろいろ分析

### 異常処理ポリシーの個別設定

`dblog_scan` では、スキャン中に異常を検出した場合の処理方針を  
3つのエラーパターンごとに**個別に設定**できます。

すべてのポリシーはデフォルトでは `report` に設定されていますが、  
呼び出し元が必要に応じて変更できます。

---

#### 設定できるポリシーの種類

| エラーパターン | 説明 | 対応する設定メンバ | デフォルト値 |
|----------------|------|--------------------|--------------|
| nondurable | durable epoch を超えたスニペット | `process_at_nondurable_` | `report` |
| truncated | 途中で切れた（不完全な）スニペットやエントリ | `process_at_truncated_` | `report` |
| damaged | 不明な型（破損）を含むスニペット | `process_at_damaged_` | `report` |

---

#### 選択できる動作モード

すべてのパターンに共通して、選択できるモードは次の通りです。

| モード | 説明 |
|--------|------|
| ignore | 異常を検出しても無視し、スキャンを続行する |
| repair_by_mark | スニペットを無効化マーカに書き換え、論理的に無効化する |
| repair_by_cut | 該当部分からファイル末尾を物理的に切り詰める（`truncated` と `damaged` のみ） |
| report | エラーとして報告し、スキャン結果に反映する（デフォルト） |

---

#### 個別設定の方法

それぞれのポリシーは専用の setter を呼び出して個別に設定します。

```cpp
scanner.set_process_at_nondurable_epoch_snippet(dblog_scan::process_at_nondurable::repair_by_mark);
scanner.set_process_at_truncated_epoch_snippet(dblog_scan::process_at_truncated::repair_by_cut);
scanner.set_process_at_damaged_epoch_snippet(dblog_scan::process_at_damaged::ignore);
```

### 実際の設定パターンと用途

`dblog_scan` の異常処理ポリシーは、用途に応じて次のように設定されています。

---

#### 設定パターン

| シーン | nondurable | truncated | damaged | 説明 |
|--------|------------|-----------|---------|------|
| dblogutil inspect | report | report | report | 異常を検知して報告するだけ |
| dblogutil repair | repair_by_mark | repair_by_mark または repair_by_cut | repair_by_mark または repair_by_cut | `--cut` オプションで物理切り詰めかマーク修復を選択 |
| 起動/スナップショット作成 | repair_by_mark | report | report | durable を超えたスニペットのみ自動で無効化、他は報告 |

---

#### repair の `--cut` オプションについて

- `dblogutil repair` は、`--cut` を付けると途中で切れた部分や破損部分を物理的に切り詰める。
- `--cut` を付けない場合は切り詰めは行わず、スニペットの先頭を無効化マークに書き換えて対応する。

#### fail_fast について

fail_fast は、異常を検知した際に即座にスキャンを中断するかどうかを制御するオプションです。

dblogutil inspect と repair では fail_fast は false（無効）であり、異常を記録しつつスキャンを最後まで続けます。

データベースの起動やスナップショット作成では fail_fast は true（有効）で、
修復できない異常を検知した場合は即座に処理を中断して例外をスローします。

### エラーとして扱うケースまとめ

`scan_one_pwal_file` が pWAL ファイルをスキャンする際に、
どのような状況をエラーとして扱い、どう処理が分岐するかを具体的に示します。

---

#### pWAL の基本構造とスニペットの単位

- pWAL ファイルは「スニペット（epoch snippet）」の連続で構成されています。
- スニペットは必ず **`marker_begin`** または **`marker_invalidated_begin`** で始まります。
- スニペット内には 0 個以上のエントリ（`normal_entry` など）が続きます。
- スニペットの終わりは明示的なマーカはなく、**EOF** か、次の `marker_begin` または `marker_invalidated_begin` の出現で次のスニペットとみなされます。
- つまり、マーカがない箇所でエントリが現れたり、途中で途切れたりすると構造上の異常として扱われます。

---

以下に、そのような異常がどう扱われるかを具体的に示します。

#### 1. スニペット開始マーカがない

- ファイルの先頭で `marker_begin` または `marker_invalidated_begin` 以外のエントリが出現した場合。
- 例: 最初に `normal_entry` が出ると `unexpected` エラーとして扱う。
- **動作:** `fail_fast` が有効な場合は例外をスローしてスキャンを即中断する。  
  無効な場合でもエラー状態を記録し、スキャンは即終了して後続処理には進まない。

#### 2. durable epoch を超えたスニペット

- `marker_begin` のエポックが last durable epoch より大きい場合。
- `process_at_nondurable` の設定により分岐:
  - `ignore`: 無視してスキャン継続。
  - `repair_by_mark`: `marker_invalidated_begin` に書き換えて修復し、継続。
  - `report`: 異常を検知して報告し、結果としてスキャン後に例外をスローまたはエラーコードを返す。
- 無効化して継続した場合でも、スニペット内で途中切れや不明型など他の異常が発生した場合は、
  それぞれ `process_at_truncated` や `process_at_damaged` の設定に従って追加で修復や切り詰め、報告が行われる。

#### 3. 不完全（途中で切れた）エントリ

- エントリが `SHORT_*` として検出された場合。
  - これはエントリの読み込み中に **EOF に到達してしまった** ことを意味する。
  - 読みかけで途切れたエントリの種類ごとに `SHORT_normal_entry`、`SHORT_marker_begin` など複数の `SHORT_*` が定義されている。
- `process_at_truncated` の設定により分岐:
  - `ignore`: スキャンを即終了し、以降を無視する。
  - `repair_by_mark`: スニペットの先頭を `marker_invalidated_begin` に書き換えて修復し、終了する。
  - `repair_by_cut`: ファイルを物理的に切り詰めて修復し、以降の読み取りを停止する。
  - `report`: 異常を報告し、結果として例外をスローまたはエラーコードを返す。

#### 4. スニペット開始マーカ自体が途中で切れている

- `SHORT_marker_begin` が出現した場合。
- `process_at_truncated` に従って分岐（3と同様）。

#### 5. 無効化済みスニペットの開始マーカが途中で切れている

- `SHORT_marker_inv_begin` が出現した場合。
- 既に無効化されているため致命的ではないが、`process_at_truncated` に従って処理。
- `ignore` 以外は報告してスキャンを終了、場合によっては例外をスロー。

#### 6. 不明な型のエントリが出現

- `UNKNOWN_TYPE_entry` が出現した場合。
- スニペット先頭の場合は `unexpected` として扱い、例外をスローして即中断。
- スニペット内の場合は `process_at_damaged` に従う:
  - `ignore`: 異常部分を無視して終了。
  - `repair_by_mark`: スニペットの先頭を無効化し終了。
  - `repair_by_cut`: ファイル末尾を切り詰めて終了。
  - `report`: 異常を報告し、結果として例外をスローまたはエラーコードを返す。

---

#### 7. その他の予期しないエントリ

- 想定外の型やロジックバグにより不明な状態に到達した場合。
- **動作:** `unexpected` として例外をスローして即中断。

---

### 共通事項

- 修復が行われない場合や `fail_fast` が有効な場合は、ほとんどのケースで例外がスローされる。
- 修復が成功すればスキャンは続行または正常終了するが、エラー報告用の結果オブジェクト（`parse_error`）に状態が反映される。

このように、異常を検知した後の挙動は「スキャン継続／修復して終了／例外をスローして中断」が
ポリシー設定に応じて切り替わる仕組みになっている。

### エラー処理内容の種類

`scan_one_pwal_file` のエラー処理は、異常を検知した後に行われる対応として  
大きく次の 4 種類に分類できます。

#### 1. スキャンの即時中断

- `fail_fast` が有効、または想定外の状態（`unexpected`）を検知した場合に発動。
- 異常を検知した時点で `aborted` を立ててループを終了し、呼び出し元に例外またはエラーコードを返す。

**代表例**  
- スニペット開始マーカがない  
- 不明型エントリが先頭に出現  
- `fail_fast` が有効で途中切れを許さない場合

#### 2. 修復して継続

- 構造異常が論理的に修復可能な場合、スニペット先頭を `marker_invalidated_begin` に書き換え無効化する。
- 修復後はスニペット内のデータを無視してスキャンを継続。

**代表例**  
- durable epoch 超えのスニペット → `repair_by_mark`
- 途中切れエントリ → `repair_by_mark`

#### 3. ファイルを切り詰める

- 途中で切れたデータを物理的に除去する必要がある場合に `repair_by_cut` を選択。
- ファイルを現在位置で `resize_file` し、以降のスキャンを終了する。

**代表例**  
- `SHORT_*` 検出 → `repair_by_cut`
- 不明型エントリ → `repair_by_cut`

#### 4. エラーとして報告のみ

- 修復や切り詰めを行わず、異常を `parse_error` に記録して結果として呼び出し元に返す。
- スキャン自体は途中で終了するがファイル内容には変更なし。

**代表例**  
- durable epoch 超え → `report`
- 途中切れエントリ → `report`
- 不明型エントリ → `report`

### 処理まとめ表

| 処理 | 内容 | 代表的な設定例 |
|------|----------------------------|----------------------|
| スキャン即時中断 | 異常を検知すると即例外または中断 | `fail_fast` 有効、`unexpected` |
| 修復して継続 | マーカを書き換えて無効化し継続 | `repair_by_mark` |
| ファイルを切り詰める | 該当部分を物理削除して終了 | `repair_by_cut` |
| 報告のみ | 修復せず異常を報告して終了 | `report` |

### parse_error コードの意味と使い分け

`scan_one_pwal_file` はスキャン中に発生した異常状態を `parse_error` のコードで呼び出し元に返します。  
このコードによって、異常の種類や致命度、修復可能性が表現されます。

---

#### 主な `parse_error` コード

| コード | 意味 | 典型的な使われ方 |
|--------|------|------------------|
| `ok` | 異常なし | 正常終了 |
| `repaired` | 異常を検知したが修復済み | `repair_by_mark` や `repair_by_cut` |
| `broken_after_tobe_cut` | 部分破損を物理切り詰め予定 | `repair_by_cut` を選んだ場合に一時的にセット |
| `broken_after_marked` | 部分破損を論理無効化で修復済みだが残骸あり | `repair_by_mark` で無効化後、内部的に残骸を示す |
| `nondurable_entries` | durable epoch を超えた未処理スニペットあり | `process_at_nondurable = report` |
| `broken_after` | 部分破損を修復しないで報告のみ | `report` を選んだ場合 |
| `unexpected` | スニペット構造が論理的に壊れている | 先頭にマーカがない、順序異常 |
| `failed` | 修復不能な致命的エラー | durable epoch 以下の破損など、DB の永続性喪失レベル |

---

#### 使い分けのポイント

- `unexpected` は構造異常（スニペット順序が論理的に破綻）を表す。
- `broken_after_*` は途中切れや不明型など「部分破損で修復の余地あり」を表す。
- `failed` は他のどのカテゴリでも救えない致命的な状態を意味し、  
  例として **durable epoch 以下の破損検知時に即セットしてスキャンを中断する** のが適切。

---

これにより、スキャンの結果がどういう状態で終わったかを  
`parse_error` を見るだけで明確に把握できます。

## バグ修正

### バグの内容

`scan_one_pwal_file` の現行実装には、**durable epoch 以下のスニペットが途中切れ（`SHORT_*`）や不明型（`UNKNOWN_TYPE_entry`）で破損している場合でも、`repair_by_mark` や `repair_by_cut` によって修復可能として処理を継続してしまう** 問題があります。

しかし durable epoch は「この epoch までのコミット結果は永続化されており、結果をユーザに返しても良い」とシステムが保証する境界です。  
このため durable epoch 以下のデータを失うことは、**トランザクションの永続性保証違反**となり、修復可能扱いにするのは設計バグです。

---

### 修正方針

durable epoch 以下のスニペットで途中切れや不明型を検知した場合は、  
**どの `process_at_*` ポリシーが設定されていても修復を許さず、即エラーとして扱う** 仕様に変更します。

#### 修正のポイント

- `marker_begin` / `marker_invalidated_begin` を読んだ時点で `current_epoch` を更新し、常にスニペットの epoch を把握する。
- 状態遷移ループ内で `SHORT_*` / `UNKNOWN_TYPE_entry` を検知した際に、`current_epoch <= durable_epoch` であれば：
  - `parse_error` に `corrupted_durable_snippet`（または `failed`）をセットする。
  - 即スキャンを中止し、そのファイルは修復不可として扱う。
  - `marker_invalidated_begin` を読んだ場合も、その epoch が durable epoch 以下なら同様にエラー扱いにする。
- `repair_by_mark` / `repair_by_cut` は durable epoch 以下では発動しない。
- durable epoch を超えるスニペットに対しては、従来どおりポリシーに従って修復を許可する。

#### `parse_error` コードの追加

このケースを区別してログで原因がすぐ分かるように、  
`parse_error` に新しいコード `corrupted_durable_snippet` を追加し、  
「永続性保証違反の修復不能エラー」であることを明示する。

#### `fail_fast` の扱い

- durable epoch 以下の破損は **`fail_fast` の設定にかかわらず** そのファイル単位で即中断する。
- `fail_fast = true` の場合は、ファイル単位の致命エラーが発生した時点で全体スキャンを即中断する。
- `fail_fast = false` の場合は、ファイル単位でエラーがあっても他の pWAL のスキャンは続行する（`inspect` 用途）。

この修正方針により、**durable epoch 以下の不整合を誤って修復可能扱いにしない** ことで、  
DB の永続性保証を正しく守れる状態にします。

## scan_one_pwal_file 完全フロー解説

from chatgpt

```
このセクションでは dblog_scan::scan_one_pwal_file のソースの動きを、
途中の細かい分岐も含めて、全体の流れとして整理します。

---

1) 事前準備

epoch_id_type current_epoch{UINT64_MAX};
epoch_id_type max_epoch_of_file{0};
log_entry::read_error ec{};
int fixed = 0;

log_entry e;

auto err_unexpected = [&](){
    log_entry::read_error ectmp{};
    ectmp.value(log_entry::read_error::unexpected_type);
    ectmp.entry_type(e.type());
    report_error(ectmp);
};

boost::filesystem::fstream strm;
strm.open(p, std::ios_base::in | std::ios_base::out | std::ios_base::binary);

- current_epoch はスニペットの epoch を記録。初期値は UINT64_MAX (未設定を示す番兵)。
- max_epoch_of_file はこのファイル内での最大 epoch を追跡する。
- fixed は修復の件数をカウントする。
- err_unexpected は想定外順序を報告するための共通ロジック (本来は関数化推奨)。
- strm は読み書きモードで開く。

---

2) 状態フラグ

bool valid = true;
[[maybe_unused]]
bool invalidated_wrote = true;
bool marked_before_scan{};
bool first = true;

- valid : スニペットが有効かどうか。
- invalidated_wrote : 無効化マークを書いたか (repair_by_mark 用)。
- marked_before_scan : 既に marker_invalidated_begin だった場合に true。
- first : スニペットの先頭エントリかどうか。

---

3) メインループ

while (true) {
    auto fpos_before_read_entry = strm.tellg();
    bool data_remains = e.read_entry_from(strm, ec);
    lex_token tok{ec, data_remains, e};
    bool aborted = false;

    switch (tok.value()) {
      ...
    }

    if (aborted) break;
    first = false;
}

- 1件ずつ読み取り lex_token で種類を分類する。
- aborted が true になるとループ終了。

---

4) 各トークンの分岐

■ 正常エントリ (normal_entry 系)

case normal_entry:
case normal_with_blob:
case remove_entry:
case clear_storage:
case add_storage:
case remove_storage:
  if (!first) {
    if (valid) add_entry(e);
  } else {
    err_unexpected();
    pe = unexpected;
    if (fail_fast_) aborted = true;
  }

- スニペット先頭で normal_entry が来たらエラー。
- 2件目以降かつ valid なら add_entry に渡す。

---

■ EOF

case eof:
  aborted = true;
  break;

---

■ marker_begin

case marker_begin:
  fpos_epoch_snippet = fpos_before_read_entry;
  current_epoch = e.epoch_id();
  max_epoch_of_file = max(max_epoch_of_file, current_epoch);
  marked_before_scan = false;

  if (current_epoch <= ld_epoch) {
    valid = true;
    invalidated_wrote = false;
  } else {
    switch (process_at_nondurable_) {
      case ignore: break;
      case repair_by_mark: invalidate_epoch_snippet(...); fixed++; invalidated_wrote = true; pe = repaired; break;
      case report: report_error(...); pe = nondurable_entries; break;
    }
    valid = false;
  }
  break;

- スニペット開始。epoch を更新し durable と比較。

---

■ marker_invalidated_begin

case marker_invalidated_begin:
  fpos_epoch_snippet = fpos_before_read_entry;
  current_epoch = e.epoch_id();
  max_epoch_of_file = max(max_epoch_of_file, current_epoch);
  marked_before_scan = true;
  invalidated_wrote = true;
  valid = false;
  break;

- 無効化済みスニペットなので読み飛ばし。

---

■ SHORT_* (途中切れ)

case SHORT_normal_entry:
...
case SHORT_remove_storage:
  if (first) {
    err_unexpected();
    pe = unexpected;
  } else {
    switch (process_at_truncated_) {
      case ignore: break;
      case repair_by_mark: strm.clear(); if (valid) { invalidate_epoch_snippet(...); fixed++; valid = false; } pe = broken_after_marked; break;
      case repair_by_cut: pe = broken_after_tobe_cut; break;
      case report: if (valid) { report_error(...); pe = broken_after; } else if (marked_before_scan) { pe = broken_after_marked; } else { pe = broken_after; } break;
    }
  }
  aborted = true;
  break;

- 途中切れは必ず終了。

---

■ SHORT_marker_begin, SHORT_marker_inv_begin も同様に process_at_truncated_ に従い分岐し aborted = true。

---

■ UNKNOWN_TYPE_entry

case UNKNOWN_TYPE_entry:
  if (first) {
    err_unexpected();
    pe = unexpected;
  } else {
    switch (process_at_damaged_) {
      case ignore: break;
      case repair_by_mark: strm.clear(); if (valid) { invalidate_epoch_snippet(...); fixed++; valid = false; } pe = broken_after_marked; break;
      case repair_by_cut: pe = broken_after_tobe_cut; break;
      case report: if (valid) { report_error(...); pe = broken_after; } else if (marked_before_scan) { pe = broken_after_marked; } else { pe = broken_after; } break;
    }
  }
  aborted = true;
  break;

---

5) 修復で CUT

if (pe == broken_after_tobe_cut) {
  resize_file(p, pe.fpos());
  pe = repaired;
  fixed++;
}

---

6) 戻り値

pe.modified(fixed > 0);
return max_epoch_of_file;

---

■ ポイント

- スニペット先頭で epoch を必ず把握。
- valid, invalidated_wrote, marked_before_scan で状態管理。
- process_at_* に応じて分岐し修復するか終了するかを決める。
- fail_fast が true なら想定外で即終了。

以上で scan_one_pwal_file の詳細な流れが理解できる。
```

## 修正メモ

### durable epoch 以下のmarker_invalidated_begin

```cpp
        case lex_token::token_type::marker_invalidated_begin: {
// marker_invalidated_begin : { head_pos := ...; max-epoch := max(...); valid := false } -> loop
            fpos_epoch_snippet = fpos_before_read_entry;
            max_epoch_of_file = std::max(max_epoch_of_file, e.epoch_id());
            marked_before_scan = true;
            invalidated_wrote = true;
            valid = false;
            VLOG_LP(45) << "valid: false (already marked)";
            break;
        }
```
* epochの値がdurable epoch以下であった場合、正しくエラーにする。
* このバグ修正前のデータを、修正後に読んだときに正しくエラーにするために必要

### SHORTエントリの処理

```cpp
        case lex_token::token_type::SHORT_normal_entry:
        case lex_token::token_type::SHORT_normal_with_blob:
        case lex_token::token_type::SHORT_remove_entry:
        case lex_token::token_type::SHORT_clear_storage:
        case lex_token::token_type::SHORT_add_storage:
        case lex_token::token_type::SHORT_remove_storage: {
// SHORT_normal_entry | SHORT_normal_with_blob | SHORT_remove_entry | SHORT_clear_storage | SHORT_add_storage | SHORT_remove_storage : (not 1st) { if (valid) error-truncated } -> END
            if (first) {
                err_unexpected();
                pe = parse_error(parse_error::unexpected, fpos_before_read_entry);
            } else {
                switch (process_at_truncated_) {
                case process_at_truncated::ignore:
                    break;
                case process_at_truncated::repair_by_mark:
                    strm.clear();  // reset eof
                    if (valid) {
                        invalidate_epoch_snippet(strm, fpos_epoch_snippet);
                        fixed++;
                        VLOG_LP(0) << "marked invalid " << p << " at offset " << fpos_epoch_snippet;
                        // quitting loop just after this, so no need to change 'valid', but...
                        valid = false;
                        VLOG_LP(45) << "valid: false";
                    }
                    if (pe.value() < parse_error::broken_after_marked) {
                        pe = parse_error(parse_error::broken_after_marked, fpos_epoch_snippet);
                    }
                    break;
                case process_at_truncated::repair_by_cut:
                    pe = parse_error(parse_error::broken_after_tobe_cut, fpos_epoch_snippet);
                    break;
                case process_at_truncated::report:
                    if (valid) {
                        // durable broken data, serious
                        report_error(ec);
                        pe = parse_error(parse_error::broken_after, fpos_epoch_snippet);
                    } else if (marked_before_scan) {
                        if (pe.value() < parse_error::broken_after_marked) {
                            pe = parse_error(parse_error::broken_after_marked, fpos_epoch_snippet);
                        }
                    } else {
                        // marked during inspect
                        pe = parse_error(parse_error::broken_after, fpos_epoch_snippet);
                    }
                }
            }
            aborted = true;
            break;
        }
```

* いくつかの状態は、深刻なエラー（回復不能な破損だが）、リペア処理が正しくない。リペアなどせず、エラーとして扱うべき。
* すでにこのWALがdurable epochより大きなmarker_beginを持っている場合はリペア可能、それ以外は回復不能な破損として扱うべきです。

### SHORT_marker_begin

```
        case lex_token::token_type::SHORT_marker_begin: {
// SHORT_marker_begin : { head_pos := ...; error-truncated } -> END
            fpos_epoch_snippet = fpos_before_read_entry;
            marked_before_scan = false;
            switch (process_at_truncated_) {
            case process_at_truncated::ignore:
                break;
            case process_at_truncated::repair_by_mark:
                strm.clear();  // reset eof
                invalidate_epoch_snippet(strm, fpos_epoch_snippet);
                fixed++;
                VLOG_LP(0) << "marked invalid " << p << " at offset " << fpos_epoch_snippet;
                pe = parse_error(parse_error::broken_after_marked, fpos_epoch_snippet);
                break;
            case process_at_truncated::repair_by_cut:
                pe = parse_error(parse_error::broken_after_tobe_cut, fpos_epoch_snippet);
                break;
            case process_at_truncated::report:
                report_error(ec);
                pe = parse_error(parse_error::broken_after, fpos_epoch_snippet);
            }
            aborted = true;
            break;
        }
```

* durable epoch以下のepochの場合の処理が抜けている。
* このmarkerで始まったepochがdurable_epochなのかどうか、本質的にわからないので、それを前提とする必要がある。
  * これ、結構厳しい。破損とみなすかどうか、どうしよう。
  * すでにこのWALがdurable epochより大きなmarker_beginを持っている場合はリペア可能、それ以外は回復不能な破損として扱うべき。
  * SHORTエントリは全て同じ扱いにすべき。

### SHORT_marker_inv_begin

```cpp
        case lex_token::token_type::SHORT_marker_inv_begin: {
// SHORT_marker_inv_begin : { head_pos := ... } -> END
            fpos_epoch_snippet = fpos_before_read_entry;
            marked_before_scan = true;
            // ignore short in invalidated blocks
            switch (process_at_truncated_) {
            case process_at_truncated::ignore:
                break;
            case process_at_truncated::repair_by_mark:
                strm.clear();  // reset eof
                // invalidate_epoch_snippet(strm, fpos_epoch_snippet);
                // fixed++;
                // VLOG_LP(0) << "marked invalid " << p << " at offset " << fpos_epoch_snippet;
                pe = parse_error(parse_error::broken_after_marked, fpos_epoch_snippet);
                break;
            case process_at_truncated::repair_by_cut:
                pe = parse_error(parse_error::broken_after_tobe_cut, fpos_epoch_snippet);
                break;
            case process_at_truncated::report:
                report_error(ec);
                pe = parse_error(parse_error::broken_after_marked, fpos_epoch_snippet);
            }
            aborted = true;
            break;
        }
```

* これは、SHORT_marker_beginをリペアするとできる。
* これも他のSHORTエントリと同じ扱いで良さそう。
* SHORT_marker_beginをリペア以外で起きるとrepair中の電源断とKILL
  * そもそも想定していなさそう。
  * そういうデータを使わないように対策すべきでは。

### UNKNOWN_TYPE_entry

* 今回のバグのケース

### リペア中の障害

リペア中にプロセスが死んだり、電源断が起きた時に、次にリペアできない壊れ方をする可能性がある。
かなり、いろいろな壊れ方をする可能性があり、今のやり方で良いのか議論が必要。

#### durable epochを超えるスニペット

* durable epoch を超えるスニペットは全部無効化するのが本質的に安全
* marker_endを導入し、marker_end後durable epochのmarker_begin以外のエントリがでてきたら、それ以降すべて無効として処理すべき。

## Durable Epoch 以下を破損とする修正

durable epoch より小さいSHORT_entryや、UNKNOWN_TYPE_entryを、WALの破損として扱うように修正する。

### DFAの修正

```cpp
// DFA
//  START:
//    eof                        : {} -> END
//    marker_begin               : { head_pos := ...; max-epoch := max(...); if (epoch <= ld) { valid := true } else { valid := false, error-nondurable } } -> loop
//    marker_invalidated_begin   : { head_pos := ...; max-epoch := max(...); valid := false } -> loop
//    SHORT_marker_begin         : { head_pos := ...; if (current_epoch <= ld) error-corrupted-durable else error-truncated } -> END
//    SHORT_marker_inv_begin     : { head_pos := ...; error-truncated } -> END
//    UNKNOWN_TYPE_entry         : { if (current_epoch <= ld) error-corrupted-durable else error-broken-snippet-header } -> END
//    else                       : { err_unexpected } -> END
//  loop:
//    normal_entry               : { if (valid) process-entry } -> loop
//    normal_with_blob           : { if (valid) process-entry } -> loop
//    remove_entry               : { if (valid) process-entry } -> loop
//    clear_storage              : { if (valid) process-entry } -> loop
//    add_storage                : { if (valid) process-entry } -> loop
//    remove_storage             : { if (valid) process-entry } -> loop
//    eof                        : {} -> END
//    marker_begin               : { head_pos := ...; max-epoch := max(...); if (epoch <= ld) { valid := true } else { valid := false, error-nondurable } } -> loop
//    marker_invalidated_begin   : { head_pos := ...; max-epoch := max(...); valid := false } -> loop
//    SHORT_normal_entry         : { if (valid && current_epoch <= ld) error-corrupted-durable else if (valid) error-truncated } -> END
//    SHORT_normal_with_blob     : { if (valid && current_epoch <= ld) error-corrupted-durable else if (valid) error-truncated } -> END
//    SHORT_remove_entry         : { if (valid && current_epoch <= ld) error-corrupted-durable else if (valid) error-truncated } -> END
//    SHORT_clear_storage        : { if (valid && current_epoch <= ld) error-corrupted-durable else if (valid) error-truncated } -> END
//    SHORT_add_storage          : { if (valid && current_epoch <= ld) error-corrupted-durable else if (valid) error-truncated } -> END
//    SHORT_remove_storage       : { if (valid && current_epoch <= ld) error-corrupted-durable else if (valid) error-truncated } -> END
//    SHORT_marker_begin         : { if (current_epoch <= ld) error-corrupted-durable else error-truncated } -> END
//    SHORT_marker_inv_begin     : { error-truncated } -> END
//    UNKNOWN_TYPE_entry         : { if (valid && current_epoch <= ld) error-corrupted-durable else if (valid) error-damaged-entry } -> END
```

##  marker_endの追加

### log_channel::end_sessionの修正

log::log_channel::end_session ストリームをflush/syncする処理の前に、marker_endを追加する。

### DFAの修正

修正後のDFA

```cpp
// DFA
//
//  NOTE:
//    - This module currently fully accepts the old WAL format.
//    - In the old format, epoch snippets do not have explicit `marker_end` entries.
//      Each snippet ends implicitly when the next `marker_begin` or EOF appears.
//    - In the new format (future), each snippet *must* end with a `marker_end`.
//      If `marker_end` is missing in durable range, it will be treated as corruption.
//    - For now, this DFA does not enforce `marker_end` for durable epochs.
//      So `marker_begin` always implicitly closes any previous snippet.
//
//  START:
//    eof                        : {} -> END
//    marker_begin               : { head_pos := ...; max-epoch := max(...); if (epoch <= ld) { valid := true } else { valid := false, error-nondurable } } -> loop
//    marker_invalidated_begin   : { head_pos := ...; max-epoch := max(...); valid := false } -> loop
//    SHORT_marker_begin         : { head_pos := ...; if (current_epoch <= ld) error-corrupted-durable else error-truncated } -> END
//    SHORT_marker_inv_begin     : { head_pos := ...; error-truncated } -> END
//    marker_end                 : { error-unexpected } -> END
//    SHORT_marker_end           : { error-unexpected } -> END
//    UNKNOWN_TYPE_entry         : { if (current_epoch <= ld) error-corrupted-durable else error-broken-snippet-header } -> END
//    else                       : { err_unexpected } -> END
//
//  loop:
//    normal_entry               : { if (valid) process-entry } -> loop
//    normal_with_blob           : { if (valid) process-entry } -> loop
//    remove_entry               : { if (valid) process-entry } -> loop
//    clear_storage              : { if (valid) process-entry } -> loop
//    add_storage                : { if (valid) process-entry } -> loop
//    remove_storage             : { if (valid) process-entry } -> loop
//    eof                        : {} -> END
//    marker_begin               : { head_pos := ...; max-epoch := max(...); if (epoch <= ld) { valid := true } else { valid := false, error-nondurable } } -> loop
//    marker_invalidated_begin   : { head_pos := ...; max-epoch := max(...); valid := false } -> loop
//    marker_end                 : { mark end of snippet; reset state } -> loop
//    SHORT_normal_entry         : { if (valid && current_epoch <= ld) error-corrupted-durable else if (valid) error-truncated } -> END
//    SHORT_normal_with_blob     : { if (valid && current_epoch <= ld) error-corrupted-durable else if (valid) error-truncated } -> END
//    SHORT_remove_entry         : { if (valid && current_epoch <= ld) error-corrupted-durable else if (valid) error-truncated } -> END
//    SHORT_clear_storage        : { if (valid && current_epoch <= ld) error-corrupted-durable else if (valid) error-truncated } -> END
//    SHORT_add_storage          : { if (valid && current_epoch <= ld) error-corrupted-durable else if (valid) error-truncated } -> END
//    SHORT_remove_storage       : { if (valid && current_epoch <= ld) error-corrupted-durable else if (valid) error-truncated } -> END
//    SHORT_marker_begin         : { if (current_epoch <= ld) error-corrupted-durable else error-truncated } -> END
//    SHORT_marker_inv_begin     : { error-truncated } -> END
//    SHORT_marker_end           : { error-truncated } -> END
//    UNKNOWN_TYPE_entry         : { if (valid && current_epoch <= ld) error-corrupted-durable else if (valid) error-damaged-entry } -> END
```

### DFAの修正に関するコメント

本修正では、WAL のスニペット終端を明示的に示す `marker_end` を新たに導入し、  
旧フォーマットとの互換性を維持しつつ、将来的な仕様強化に対応可能な構造としている。

現行の DFA は以下の設計方針に基づいている。

- **旧フォーマットの互換性**  
  旧フォーマットでは `marker_end` が存在せず、スニペットは次の `marker_begin` または EOF により  
  暗黙的に終了していた。このため、`marker_begin` は常に前のスニペットを自動的に閉じる役割を兼ねている。

- **新フォーマットへの移行**  
  新フォーマットでは、各スニペットを `marker_begin` から `marker_end` で明示的に区切ることを前提とする。  
  将来的には、`marker_end` が欠落している durable 範囲のスニペットは破損として扱い、  
  修復可能とはみなさない設計とする。

- **途中切れ（SHORT_系）および UNKNOWN_TYPE_entry の扱い**  
  途中切れや型不明の断片は、直前のスニペット（`current_epoch`）に属するとみなし、  
  durable epoch 内で発生した場合には回復不能な破損として直ちに中断する。  
  これにより、永続化保証を損なわないことを担保する。

- **`marker_end` 後のエントリ処理の考え方**  
  `marker_end` を読んだ後は、次に現れるのは必ず `marker_begin`（または `marker_invalidated_begin`）であるべきである。  
  それ以外のエントリが出現した場合は構造異常であり、適切に検出されなければならない。

- **フラグによる状態管理**  
  この判定のため、内部状態として `first` フラグを使用している。  
  `marker_end` を読んだ直後は `first` を `true` にすることで、論理的に「次のスニペット開始を待つ状態」であることを示している。

- **`first` フラグによる分岐の役割**  
  `first` フラグにより、`marker_begin` 以外のエントリが来た場合でも、
  「これはスニペット先頭の異常か、途中のエントリか」を区別して処理が変わるようになっている。  
  これにより、`SHORT_marker_begin` や `UNKNOWN_TYPE_entry` は直前スニペットの閉じ漏れを補足でき、  
  `normal_entry` などは即 `unexpected` として扱われる。

- **DFA には明示されない実装依存の制御**  
  この `first` フラグによる挙動は、DFA の状態遷移としては明示されていないが、  
  実装上は `loop` 状態内で論理的に `START` 相当を再現する重要な仕組みである。

- **将来的な強化計画**  
  将来的には、外部設定により「旧フォーマットとして扱う最終 epoch」を明示的に指定できるようにする予定である。  
  この境界値以降のエポックについては新フォーマットとして扱い、各スニペットの終端に `marker_end` が必ず存在することを要求する。  
  もし新フォーマットと認識される範囲内で `marker_end` が欠落し、`marker_begin` が出現した場合は構造破損として即エラーとする。  
  これにより、パーサの状態遷移は旧フォーマットと新フォーマットを混在させたままでも正確に判別でき、  
  永続性保証の厳格化と段階的な仕様強化が両立される設計となる。  
  
  さらに `marker_end` には将来的に CRC を付与し、当該スニペット全体に物理的な破損がないことを  
  検証可能とする拡張も検討している。ただし、この CRC チェック機能については現時点では実装しない。
- **新フォーマットへの移行**  
  新フォーマットでは、各スニペットを `marker_begin` から `marker_end` で明示的に区切ることを前提とする。  
  将来的には、`marker_end` が欠落している durable 範囲のスニペットは破損として扱い、  
  修復可能とはみなさない設計とする。

- **途中切れ（SHORT_系）および UNKNOWN_TYPE_entry の扱い**  
  途中切れや型不明の断片は、直前のスニペット（`current_epoch`）に属するとみなし、  
  durable epoch 内で発生した場合には回復不能な破損として直ちに中断する。  
  これにより、永続化保証を損なわないことを担保する。

- **`marker_end` 後のエントリ処理の考え方**  
  `marker_end` を読んだ後は、次に現れるのは必ず `marker_begin`（または `marker_invalidated_begin`）であるべきである。  
  それ以外のエントリが出現した場合は構造異常であり、適切に検出されなければならない。

- **フラグによる状態管理**  
  この判定のため、内部状態として `first` フラグを使用している。  
  `marker_end` を読んだ直後は `first` を `true` にすることで、論理的に「次のスニペット開始を待つ状態」であることを示している。

- **`first` フラグによる分岐の役割**  
  `first` フラグにより、`marker_begin` 以外のエントリが来た場合でも、
  「これはスニペット先頭の異常か、途中のエントリか」を区別して処理が変わるようになっている。  
  これにより、`SHORT_marker_begin` や `UNKNOWN_TYPE_entry` は直前スニペットの閉じ漏れを補足でき、  
  `normal_entry` などは即 `unexpected` として扱われる。

- **DFA には明示されない実装依存の制御**  
  この `first` フラグによる挙動は、DFA の状態遷移としては明示されていないが、  
  実装上は `loop` 状態内で論理的に `START` 相当を再現する重要な仕組みである。


- **将来的な強化計画**  
  将来的には、外部設定により「旧フォーマットとして扱う最終 epoch」を明示的に指定できるようにする予定である。  
  この境界値以降のエポックについては新フォーマットとして扱い、各スニペットの終端に `marker_end` が必ず存在することを要求する。  
  もし新フォーマットと認識される範囲内で `marker_end` が欠落し、`marker_begin` が出現した場合は構造破損として即エラーとする。  
  これにより、パーサの状態遷移は旧フォーマットと新フォーマットを混在させたままでも正確に判別でき、  
  永続性保証の厳格化と段階的な仕様強化が両立される設計となる。  
  
  さらに `marker_end` には将来的に CRC を付与し、当該スニペット全体に物理的な破損がないことを  
  検証可能とする拡張も検討している。ただし、この CRC チェック機能については現時点では実装しない。



### テストのマトリクス


| テスト対象                                   | durable_epoch < epoch | durable_epoch == epoch | durable_epoch > epoch | 備考 |
|----------------------------------------------|-----------------------|------------------------|-----------------------|------|
| **1. marker_end のみ**                       | nondurable_entries    | ok                     | ok                    | durable_epoch 未満なら未コミットなので破損 |
| **2. marker_end の後に normal_entry**        | nondurable_entries/unexpected            | unexpected             | unexpected            | 不正追記は破損、durable含むなら修復不能 |
| **3. marker_end の後に marker_begin**        | nondurable_entries/nondurable_entries    | ok/nondurable_entries                  | ok/ok                 | 後続の marker_begin は新しいスニペット。epoch は連続している想定 |
| **4. marker_end の後に marker_inv_begin**    | nondurable_entries    | ok                  | ok                 | 修復済みのスニペットとして正常扱い、後続 epoch は連続想定 |
| **5. marker_end の後に SHORT_entry**         | nondurable_entries/unexpected            | unexpected             | unexpected            | 終了済みの後に SHORT_* は論理破綻 |
| **6. SHORT_marker_end のみ**                 | broken_after          | corrupted_durable_entries | corrupted_durable_entries | 不完全終了を修復 or 修復不能 |

**補足**

- `ok/broken_after` は「前半スニペット / 後半スニペット」の解析結果を表す
- 後続スニペットの epoch は連続（例: epoch+1）で想定する
- テスト対象が2つのスニペットを持つ場合、このテーブルでの **`epoch`** は前半のスニペットの epoch を指す
- ここでの **`marker_end`** は「`marker_begin` で始まり、`marker_end` で正常に閉じられた *スニペットの終了マーカー*」を意味します。
- **`SHORT_marker_end`** も同様に、`marker_begin` で始まり `SHORT_marker_end` で終わるはずの *スニペットの終了マーカー* を指します。
- **3. marker_end の後に marker_begin** と **4. marker_end の後に marker_inv_begin** は、どちらも「`marker_begin` または `marker_inv_begin` で始まり、正常に `marker_end` で閉じられた次のスニペット」であることが前提です。そうでない場合（例えば未終了や壊れた場合）は、このテストの前提外として別エラーが発生します。


- **ok**
  - WAL が完全に正常で、`durable_epoch` までのエントリが正しく揃っている。
  - 修復は不要。

- **nondurable_entries**
  - `durable_epoch` より新しい完全なスニペットが存在する。
  - `repair_by_mark` や `repair_by_cut` で修復する必用がある。

- **broken_after**
  - WAL の末尾など一部が途中で切れていたり、予期しない構造になっている。
  - `durable_epoch` より新しい部分なので、`repair_by_mark` や `repair_by_cut` で修復可能。
  - `durable_epoch` を超える部分なら切り離せばOK、超えない場合は破損扱い。

- **corrupted_durable_entries**
  - `durable_epoch` 以内のエントリが途中で欠損している。
  - コミット済みのデータが壊れているので修復不能。
  - 即エラーで停止。


- **unexpected**
  - 想定外の構造が出現した状態。
  - コードの前提条件違反、論理バグ、ファイルフォーマット不一致の可能性。


## 🔍 モード別動作表

| ステータス                    | inspect の挙動      | repair_by_mark の挙動 | repair_by_cut の挙動 | 備考 |
|-------------------------------|---------------------|-----------------------|----------------------|------|
| **ok**                        | ok                  | ok                    | ok                   | 完全に正常なので何もしない |
| **broken_after**              | broken_after        | repaired (mark)       | repaired (cut)       | durable_epoch を超えた未コミット部分を修復 |
| **corrupted_durable_entries** | corrupted_durable_entries | corrupted_durable_entries | corrupted_durable_entries | 修復不能。即エラー |
| **unexpected**                | unexpected (abort)  | unexpected (abort)    | unexpected (abort)   | 修復不能。即エラー |

---

### ✅ 意味

- **inspect**  
  現状を解析するのみで修復はしない。ステータスそのままを返す。

- **repair_by_mark**  
  `broken_after` ならマーカーを挿入して無効化（論理切断）。`ok` は何もしない。

- **repair_by_cut**  
  `broken_after` なら durable_epoch 超の未コミット部分を物理的に切り詰める。`ok` は何もしない。

- **corrupted_durable_entries** / **unexpected**  
  どのモードでも修復できないので即エラー。

---

### ✏️ 対応付けの位置づけ

- この表は **どのエラーがどのモードでどの結果になるか** を示す設計仕様。
- 実際には、例えば `broken_after` なら inspect は `broken_after` と返すが、 `repair_by_mark` では `repaired (mark)` となる。


## 0fill位置パターンテスト

* scan_one_pwal_file の破損検出ロジックが 途中の 0fill（UNKNOWN_TYPE_entry） を適切に検知し、
* durable epoch 内は修復不能 / durable epoch 超は repair by mark / cut で除去可能
となることを検証する。
* 0fill の位置と前後の構造に応じて、DFA と実装の分岐が仕様通り動作するかを保証する。


* テストシナリオ


| シナリオ | 内容 | durable_epoch < epoch | durable_epoch == epoch | durable_epoch > epoch |
|----------|------|-----------------------|------------------------|-----------------------|
| ファイル全体 0fill | ファイル先頭から全て 0x00 | unexpected | unexpected | unexpected |
| marker_begin の途中から 0fill | `marker_begin` の途中で切れて残りが 0x00 | corrupted_durable_entries | corrupted_durable_entries | broken_after |
| marker_begin の直後から 0fill | 完全な `marker_begin` の直後に UNKNOWN | corrupted_durable_entries | corrupted_durable_entries | broken_after |
| marker_begin + normal_entry の途中から 0fill | `normal_entry` の途中で切れて残りが 0x00 | corrupted_durable_entries | corrupted_durable_entries | broken_after |
| marker_begin + normal_entry の後から 0fill | 正常な `normal_entry` の後、次のエントリが 0x00 | corrupted_durable_entries | corrupted_durable_entries | broken_after |
| marker_end の途中から 0fill | `marker_end` が途中で切れて残りが 0x00 | corrupted_durable_entries | corrupted_durable_entries | broken_after |


* ファイルの先頭が上の表のシナリオの場合と、正常なスニペットのあとに続く場合のテストをお行う。

* 0fill 発生パターン別 WAL スキャンの期待挙動一覧

| 構造 | durable_epoch < epoch | durable_epoch >= epoch |
|------|-----------------------|------------------------|
| ファイル全体が 0x00 で埋まっている | unexpected | unexpected |
| marker_begin の途中で切れて残りが 0fill | corrupted_durable_entries | broken_after |
| marker_begin は正常だが次が UNKNOWN | corrupted_durable_entries | broken_after |
| normal_entry が途中で切れて残りが 0fill | corrupted_durable_entries | broken_after |
| 正常な normal_entry 後、次のエントリが 0x00 | corrupted_durable_entries | broken_after |
| marker_end が途中で切れて残りが 0fill | corrupted_durable_entries | broken_after |

## 0fillされたデータのリペア

### 概要

0fillされたデータは、unexpectedとして扱われるが、0fillが開始する位置によっては
現行ロジックで修復が不可能なケースがある。

具体的には、ファイルの先頭から 0fill が始まる場合と、marker_endの直後から0fillが始まる場合である。

このケース現在のコードでは次のように扱われている。

```cpp
        case lex_token::token_type::UNKNOWN_TYPE_entry: {
            // UNKNOWN_TYPE_entry : (not 1st) { if (valid && current_epoch <= ld) error-corrupted-durable else error-damaged-entry } -> END
            // UNKNOWN_TYPE_entry : (1st) { error-broken-snippet-header } -> END
            if (first) {
                err_unexpected();  // FIXME: error type
                pe = parse_error(parse_error::unexpected, fpos_before_read_entry);
            } else if (valid && current_epoch <= ld_epoch) {
```

無条件で、parse_error::unexpected を返していて、リペアされない。


実は、次の方法で修復可能である。

repair by mark => 0fillされた最初の位置にmarker_invalidated_beginを書き込む
repair by cut => 0fillされた最初の位置からカットする。


### SHORT_maker_beginの扱い

上の対応で、marker_invalidated_beginを書き込んでも、
次のWALのREADでSHORT_marker_beginとみなされると破損しているとして
扱われる。これだと、repair by markでリペアしたことにならない。

現行のSHORT_marker_beginの処理は過剰なので、単に無視するように修正する。