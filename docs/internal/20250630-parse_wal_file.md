# parse_wal_file.cppの分析

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

fail_fast は、異常を検知した際に即座にスキャンを中断するかどうか を制御するオプションです。

dblogutil inspect と repair では fail_fast は false（無効）であり、異常を記録しつつスキャンを最後まで続けます。

データベースの起動やスナップショット作成では fail_fast は true（有効）で、
修復できない異常を検知した場合は即座に処理を中断して例外をスローします。

### エラーとして扱うケースまとめ

`scan_one_pwal_file` が pWAL ファイルをスキャンする際に、
どのような状況をエラーとして扱い、どう処理が分岐するかを具体的に示します。

---

#### pWAL の基本構造とスニペットの単位

- pWAL ファイルは「スニペット（epoch snippet）」の連続で構成されています。
- スニペットはかならず **`marker_begin`** または **`marker_invalidated_begin`** で始まります。
- スニペット内には 0 個以上のエントリ（`normal_entry` など）が続きます。
- スニペットの終わりは明示的なマーカはなく、**EOF** か、次の `marker_begin` または `marker_invalidated_begin` の出現で次のスニペットと見なされます。
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
