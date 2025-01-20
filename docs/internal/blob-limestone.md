# limestoneのBLOB対応

## はじめに

本ドキュメントはLimestoneのBLOB対応について、開発項目のリストアップと
開発スケジュールの策定を目的としています。


## 参照ドキュメント

* 資料1: [PR #1086:draft: IPCでのBLOBの授受と、永続化に関するデザイン](https://github.com/project-tsurugi/tsurugi-issues/pull/1086)


## Limestone APIの追加、修正

[Limestoneに追加するAPi](blob-api.md)を参照。


### BLOB参照管理

* BLOB参照は、実態は64bitの整数値である。
  * BLOB_IDとして内部的にはシリアル値を使用する
  * 部向けにはBLOB参照作成時にユニークな値が生成されることのみを保証する(シリアル値であることも保証しない)。
  * シリアル値はDBの再起動があっても一意であることが保証されなければならない
    * 起動時に使用済みのBLOB参照の最大値を取得し、その値+1をシリアル値の開始値とする
* BLOB参照からBLOBファイルのパスを決定する
  * LOGDIRの下にblobというディレクトリを作成し、そのしたに作成するのがデフォルト
    * 要議論、blobディレクトリをログディレクトリと別のパス、別のファイルシステムに作成可能すべきでは(設定可能にする)
  * 大量のファイルが1ディレクトリに作成されないように、ディレクトリ構造を考慮する
  * BLOBファイルのパス(バスの一部かもしれない) から、BLOB参照が取得できたほうが良い。
    * 現状必須ではないが、後で必要になりそう。

### BLOBリスト付きのエントリ

* PWALに対してBLOBリスト付きのエントリをRead/Writeする機能。
  * entryタイプの追加
  * entryタイプに応じた処理の追加

### コンパクション

* コンパクション境界の変更
  * オンラインコンパクションでのコンパクションの境界を変更する。オフラインコンパクションは現状通り。
    * 現実装は、同一キーのエントリの中で最新のエントリを残し、そのたのエントリを削除
    * これを、`datastore::switch_available_boundary_version(write_version_type version)`で通知されるwrite_versionベースの判断に変更する。
      * 同一キーのエントリの中で、これによって通知されたバージョンより古いエントリの中で最新のエントリ以外のエントリを削除できる。
      * この通知がない場合、境界が不明なのでコンパクションを行わない(これはTsurugi起動時の場合)
* コンパクション時に、コンパクションファイルに含まれるBLOBのインデックスを作成する。こちらは、オンラインコンパクション、オフラインコンパクションのどちらでも必要。
  * BLOBインデックスファイルは、コンパクション済みファイルと1対1に対応する。
    * コンパクション済みファイルファイルと拡張子以外は同じで、拡張子が異なる。
  * BLOBインデックスファイルには以下の情報が必要。
    * BLOB参照
    * 現状、BLOB参照だけがあれば問題なさそうだが、設計時に他の情報も必要になりそうなので適宜追加する。
  * BLOB参照をキーに検索できる必要があるかもしれない。ソートして書くだけでもOKかもしれない。固定長でソートして書いておけば、2分木検索できるのでそれでOKかもしれない。

* コンパクション時のガーベージコレクション
  * コンパクション時に削除されたエントリが含むBLOB参照に対応するBLOBファイルを削除する。

### 起動処理

* 起動時にBLOBファイルのガベージコレクションを行う。具体的な動作は以下の通り。
  * BLOGファイルのディレクトリを操作し、BLOBファイルのリストを作成する -> (1)
  * 起動時のスナップショットファイル作成と併せて、PWALファイルのエントリに含まれるBLOB参照を取得する -> (2)
  * コンパクション済みファイルのエントリに対して(2)と同様の処理を行い、BLOB参照を取得する -> (3)
    * コンパクション時にBLOB参照のリストのインデックスを作成しちる場合、そのインデックスを使用する
  * (1) - (2) - (3)を削除する。
* 削除処理が完了するまで起動処理完了をまたせるのではなく、起動完了後にバックグランドで削除処理を行っても良い。
  * この機能はベンチマーク測定時の邪魔になるのでオプションで抑制できると良い。


### フルバックアップ
* BLOBファイルをlogdirに置くことを前提にする。
* BLOBファイルをバックアップファイルのリストに含めるようにする。
* BLPBファイルの数が2^31を超えても問題ないか、ロジックを確認する。
* ハードリンクを使用する場合、バックアップにハードリンクを含める仕組みが必要。

## ファイルフォーマット変更

BLOB対応のファイルフォーマットにする。

* Manifestのバージョン変更
* PWALファイル、コンパクション済みファイルのフォーマット変更
  * BLOBリスト付きのエントリが追加になる。
  * 旧バージョンのデータは新バージョンで取り扱い可能
  * 新バージョンのデータは旧バージョンで取り扱い不可
* BLOBインデックス
  * 旧バージョンには存在しない
    * 新バージョンで旧バージョンのデータを読んだときどうするのか
      * 起動時にBLOBインデックスを作成する。
      * GC時にインデックスを作成する。
      * BLOBインデックスなしでも動作するようにする。
        * これで良さそう。

* 新バージョンから旧バージョンへのコンバージョンツール
  * 何案か考えられる
    * 用意しない。
    * BLOBが含まれていないければコンバージョン可能、BLOBが含まれているかのチェックを行わない。
      * この場合マニフェストファイルの書き換えと、不要ファイルの削除のみでOK
      * BLOBを含むエントリがあると、実行時エラー。データの復旧不可能。
      * Limestoneとしては問題なくある変えても上位でうまく扱えない可能性がある。
    * 上記に加えて、BLOBリストの有無のチェックを行う。BLOBリストを含むエントリがあれば、コンバージョンに失敗する。
      * Limes



### 差分バックアップ

* 今回の工数見積の対象外

##　その他

* LimestoneはエントリにBLOBが含むか含まないかを、エントリ追加時のBLOBリストの有無で判断する。
  * エントリ自体が含むBLOB参照を読んでなにかすることはない。
    * Limestoneがエントリの内容に関知せず単なるバイナリとして扱うことを維持する。
  * 小さいBLOBをBLOBファイルに書かずにエントリに含めた場合Limestoneからは、当該BLOBデータは存在しないのと同じようにあるかうである。


## タスク

[対応するIssue](https://github.com/project-tsurugi/tsurugi-issues/issues/1116)

現時点でIssueのタスク


このドキュメントの現時点(2024/01/16) のタスク


* BLOB 仮登録/参照 API
  * BLOB参照の採番機構
  * BLOB関連APIのヘッダファイルの作成
  * BLOB参照と、BLOBファイルのパスの対応付け
  * BLOB APIの追加、修正
    * `std::unique_ptr<blob_pool> datastore::acquire_blob_pool()`
    * `blob_file datastore::get_blob_file(blob_id_type reference)`
    * `void switch_available_boundary_version(write_version_type version)`
    * `class blob_pool`
    * `class blob_file`
    * 不要となったメソッド、クラスの削除
* BLOB 永続化操作関連
  * ファイルフォーマットの変更
    * マニフェストのバージョン変更
  * PWALに対してBLOBリスト付きのエントリをRead/Writeする機能。
    * entryタイプの追加
    * entryタイプに応じた処理の追加
  * BLOB APIの追加、修正
      * `log_channel::add_entry`
* コンパクション時のGC関連
  * コンパクション境界の変更
  * コンパクション時のインデックス作成
  * コンパクション時のGC
* 起動処理
  * 起動時のBLOB参照の初期値の取得
  * 起動時のGC
* フルバックアップ
  

## WBS

* API仕様の確定、ヘッダファイルの提供: 〜1/17
  * これがないと、他モジュールがコンパイル不可
* 基本機能実装: 〜1/28
  * いろいろ制限があるが、スモークテストができるようになる。
    * 再起動するとBLOBが使えなくなる。
      * 新しくつくることも、BLOBにアクセスすることもできない。
    * GCがないので、BLOBが増え続ける。
    * オンラインバックアップ非対応
  * BLOB 仮登録/参照 API:
  * BLOB参照関連:
    * BLOB参照の採番機構
    * BLOB参照と、BLOBファイルのパスの対応付け
  * BLOB永続化関連
    * ファイルフォーマットの変更:
    * PWALに対してBLOBリスト付きのエントリをRead/Writeする機能:
* 再起動対応: 〜1/31
  * 再起動後にもBLOBにアクセスできる。
  * 起動時のBLOB参照の初期値の取得:
* 起動時のGC対応: 〜2/7 
  * コンパクション時のインデックス作成
  * 起動時のGC
* コンパクション時のGC対応: 〜2/14
  * switch_available_boundary_version()の呼びだしが、呼び出し側で実装されないと。コンパクション時のGC不可。
  * コンパクション境界の変更
  * コンパクション時のGC
* フルバックアップ: 〜2/21


## 議論がひつようなこと

### GCをどこまで実現する。

* GCなし
* 起動時にのみGCする。
* 起動時に加えて、コンパクション時にGCする。
  * 上位から、switch_available_boundary_version()で通知が必要。
 
### コンバージョンツール

* BLOB対応版のデータファイルをBLOB非対応版へコンバートするコンバージョンツールを提供するのか。
* BLOBを含むデータのダウンコンバートは無理
  * BLOBを含むかどうかのチェックも難しい
    * Limestone単独では、そのエントリがBLOBを含むかどうか判断できない。
* Limestoneとして互換性があっても、上位で互換性があるかわからない。

### BLOB対応版と、非対応版の共存

 * BLOB対応版はファイルフォーマット変更が必要になる。
 * おそらくBLOB対応版とBLOB非対応版の2ラインが必要になる。
 * BLOB対応版をMasterに取り込んでしまうと、BLOB非対応版でのBugFixで困ることになるので、ポリシーを決めたい。
   * BLOB対応版をブランチで開発し、BugFixなどはMasterで行い。必要に応じてブランチに取り込む。
   * BLOB対応版をマスターで開発し、BLOB非対応版のBugFixなどはブランチで行う。
 * 別案として、BLOBサポートの有効/無効を切り替えられるようにし、デフォルトでBLOBサーパートを無効、
   ユーザにはBLOBサポート無効版をつかってもらい、BLOB開発にはBLOBサポート有効にして使用する。
   BLOB対応完了後は、BLOBサポートの有効/無効の切り替えを廃止し、常に有効にする。


### key, valueのサイズについて

* 現行のlimestoneのPWALはkey, valueのサイズを32bitで扱っている。これで問題ないか。
  * BLOB自体は、32bitを超えうるが、PWALに32bit以上のデータを各ケースに対応できない。


## エントリについて

WALにBLOBリスト付きのエントリを使用可能にするにあたって、エントリの仕様を考える。

当面は、normal_entryをそのまま、BLOBリスト付きのエントリに使用可能化を検討する。

### enttry_type

log_entryクラス内で次のように定義されている。

```
enum class entry_type : std::uint8_t {
    this_id_is_not_used = 0,
    normal_entry = 1,
    marker_begin = 2,
    marker_end = 3,
    marker_durable = 4,
    remove_entry = 5,
    marker_invalidated_begin = 6,
    clear_storage = 7,
    add_storage = 8,
    remove_storage = 9,
};
```

### read_error クラス

logn_entryクラス内で次のように定義されている。

```
class read_error {
public:
    enum code {
        ok = 0,
        // warning
        nondurable_snippet = 0x01,
        // error
        short_entry = 0x81,
        // unknown type; eg. type 0
        unknown_type = 0x82,
        // unexpected type; eg. add_entry at the head of pwal file or in epoch file
        unexpected_type = 0x83,
    };

    read_error() noexcept : value_(ok) {}
    explicit read_error(code value) noexcept : value_(value) {}
    read_error(code value, log_entry::entry_type entry_type) noexcept : value_(value), entry_type_(entry_type) {}

    void value(code value) noexcept { value_ = value; }
    [[nodiscard]] code value() const noexcept { return value_; }
    void entry_type(log_entry::entry_type entry_type) noexcept { entry_type_ = entry_type; }
    [[nodiscard]] log_entry::entry_type entry_type() const noexcept { return entry_type_; }

    explicit operator bool() const noexcept { return value_ != 0; }

    [[nodiscard]] std::string message() const {
        switch (value_) {
        case ok: return "no error";
        case nondurable_snippet: return "found nondurable epoch snippet";
        case short_entry: return "unexpected EOF";
        case unknown_type: return "unknown log_entry type " + std::to_string(static_cast<int>(entry_type_));
        case unexpected_type: return "unexpected log_entry type " + std::to_string(static_cast<int>(entry_type_));
        }
        return "unknown error code " + std::to_string(value_);
    }
private:
    code value_;
    log_entry::entry_type entry_type_{0};
};
```

### private field

以下のプライベートフィールドを持つ。

* entry_type_
  * エントリタイプ
* epoch_id_
  * エポックID
* key_sid_
  * キー値
* value_etc_
  * 値など(write_versonなどが含まれることがあるようだ)


### write(スタティックメソッド)

log_entryクラス内に、entry_tppe毎のwirteメソッドが定義されている。


#### Epoch関連

以下のentry_type

* marker_begin
* marker_end
* marker_durable
* marker_invalidated_begin

次のような単純な処理を行っている。

* entyry_typeの書き込み(1byte)
* epochの書き込み(64bit = 8byteをLittle Endianで書き込み)

実際のコード

 
```
static void begin_session(FILE* strm, epoch_id_type epoch) {
    entry_type type = entry_type::marker_begin;
    write_uint8(strm, static_cast<std::uint8_t>(type));
    write_uint64le(strm, static_cast<std::uint64_t>(epoch));
}
static void end_session(FILE* strm, epoch_id_type epoch) {
    entry_type type = entry_type::marker_end;
    write_uint8(strm, static_cast<std::uint8_t>(type));
    write_uint64le(strm, static_cast<std::uint64_t>(epoch));
}
static void durable_epoch(FILE* strm, epoch_id_type epoch) {
    entry_type type = entry_type::marker_durable;
    write_uint8(strm, static_cast<std::uint8_t>(type));
    write_uint64le(strm, static_cast<std::uint64_t>(epoch));
}
static void invalidated_begin(FILE* strm, epoch_id_type epoch) {
    entry_type type = entry_type::marker_invalidated_begin;
    write_uint8(strm, static_cast<std::uint8_t>(type));
    write_uint64le(strm, static_cast<std::uint64_t>(epoch));
}
```

#### normal entry

2つのシグネチャのwriteメソッドが定義されている。

```cpp
static void write(
    FILE* strm,                     
    storage_id_type storage_id,     
    std::string_view key,           
    std::string_view value,         
    write_version_type write_version
);
```

以下の順で書き込む

* entyry_type(1byte)
* keyのサイズ(32bitLE)
* valueのサイズ(32bitLE)
* storage_id(64bitLE)
* key(バイナリ)
* write_version.epoch_number(64bitLE)
* write_version.minor_write_version(64bitLE)
* value(バイナリ)


```cpp
static void write(
    FILE* strm,                
    std::string_view key_sid,  
    std::string_view value_etc 
);
```

* entyry_type(1byte)
* keyのサイズ(32bitLE) -> key_sidのサイズ - 8byte
* valueのサイズ(32bitLE) -> value_etcサイズ - 16byte
* key_sid(バイナリ)
  * sotregae_id(64bitLE)
  * key_sidのキー部分(バイナリ)
* value_etc(バイナリ)
  * write_version.epoch_number(64bitLE)
  * write_version.minor_write_version(64bitLE)
  * value(バイナリ)


#### remove_entry

```cpp
static void write_remove(
    FILE* strm,                      
    storage_id_type storage_id,      
    std::string_view key,            
    write_version_type write_version 
);
```

* entyry_type(1byte)
* keyのサイズ(32bitLE)
* storage_id(64bitLE)
* key(バイナリ)
* valeu_etc(バイナリ)

#### ストレージ操作

以下のエントリタイプ

* clear_storage
* add_storage
* remove_storage

これらのエントリタイプ共通の2つのシグエンチャのwriteメソッドがある。

```cpp
static inline void write_ope_storage_common(
    FILE* strm,
    entry_type type,
    storage_id_type storage_id,
    write_version_type write_version
)
```

以下の順で書き込む。

* entyry_type(1byte)
* storage_id(64bitLE)
* write_version.epoch_number(64bitLE)
* write_version.minor_write_version(64bitLE)



```cpp
static inline void write_ope_storage_common(
    FILE* strm,
    entry_type type,
    std::string_view key_sid,
    std::string_view value_etc
) {
    write_uint8(strm, static_cast<std::uint8_t>(type));
    write_bytes(strm, key_sid.data(), key_sid.length());
    write_bytes(strm, value_etc.data(), value_etc.length());
}
```

以下の順で書き込む。

* key_sid(バイナリ)
  * sotregae_id(64bitLE)
  * key_sidのキー部分(バイナリ) => この部分は空である想定
* value_etc(バイナリ)
  * write_version.epoch_number(64bitLE)
  * write_version.minor_write_version(64bitLE)
  * value(バイナリ) => この部分は空である想定

各エントリタイプに対して2つずつ、合計6の関数が定義されている。

```
    static void write_clear_storage(FILE* strm, storage_id_type storage_id, write_version_type write_version) {
        write_ope_storage_common(strm, entry_type::clear_storage, storage_id, write_version);
    }

    static void write_clear_storage(FILE* strm, std::string_view key_sid, std::string_view value_etc) {
        write_ope_storage_common(strm, entry_type::clear_storage, key_sid, value_etc);
    }

    static void write_add_storage(FILE* strm, storage_id_type storage_id, write_version_type write_version) {
        write_ope_storage_common(strm, entry_type::add_storage, storage_id, write_version);
    }

    static void write_add_storage(FILE* strm, std::string_view key_sid, std::string_view value_etc) {
        write_ope_storage_common(strm, entry_type::add_storage, key_sid, value_etc);
    }

    static void write_remove_storage(FILE* strm, storage_id_type storage_id, write_version_type write_version) {
        write_ope_storage_common(strm, entry_type::remove_storage, storage_id, write_version);
    }

    static void write_remove_storage(FILE* strm, std::string_view key_sid, std::string_view value_etc) {
        write_ope_storage_common(strm, entry_type::remove_storage, key_sid, value_etc);
    }
```


### writeメソッド(メンバメソッド)

switch でentry_typeに応じたstaticメソッドを呼び出している。

* 実際のコード
```
void write(FILE* strm) {
    switch(entry_type_) {
    case entry_type::normal_entry:
        write(strm, key_sid_, value_etc_);
        break;
    case entry_type::remove_entry:
        write_remove(strm, key_sid_, value_etc_);
        break;
    case entry_type::marker_begin:
        begin_session(strm, epoch_id_);
        break;
    case entry_type::marker_end:
        end_session(strm, epoch_id_);
        break;
    case entry_type::marker_durable:
        durable_epoch(strm, epoch_id_);
        break;
    case entry_type::marker_invalidated_begin:
        invalidated_begin(strm, epoch_id_);
        break;
    case entry_type::clear_storage:
        write_clear_storage(strm, key_sid_, value_etc_);
        break;
    case entry_type::add_storage:
        write_add_storage(strm, key_sid_, value_etc_);
        break;
    case entry_type::remove_storage:
        write_remove_storage(strm, key_sid_, value_etc_);
        break;
    case entry_type::this_id_is_not_used:
        break;
    }
}
```

### readメソッド

次のようにエラー処理のみ行っていて、
実処理は、read_entry_fromメソッドが行っている。

```
bool read(std::istream& strm) {
    read_error ec{};
    bool rc = read_entry_from(strm, ec);
    if (ec) {
        LOG_AND_THROW_EXCEPTION("this log_entry is broken: " + ec.message());
    }
    return rc;
}
```

### read_entry_fromメソッド


まず、entrry_typeを読む

```cpp
ec.value(read_error::ok);
ec.entry_type(entry_type::this_id_is_not_used);
char one_char{};
strm.read(&one_char, sizeof(char));
entry_type_ = static_cast<entry_type>(one_char);
if (strm.eof()) {
    return false;
}
```

あとはエントリタイプに応じた処理をする。

* エントリタイプ
  * normal_entry
* 処理
  * key長を読む(32bitLE)
  * value長を読む(32bitLE)
  * key_sidを読む
    * key_sidのデータ長は、key長 + 8byte
  * value_etcを読む
    * value_etcのデータ長は、value長 + 16byte

* エントリタイプ
  * remove_entry
* 処理
  * key長を読む(32bitLE)
  * value長を読む(32bitLE)
  * key_sidを読む
    * key_sidのデータ長は、key長 + 8byte
  * value_etcを読む
    * 16byte
    * value部がないので、wite_versionのepoch_number, minor_write_versionのみ読む
  
* エントリタイプ
  * clear_storage
  * add_storage
  * remove_storage
* 処理
  * key_sidを読む
    * このエントリタイプはkey部分は存在せず、storage_idのみが存在するので、key_sidのサイズを、ストレージIDのサイズにせっとして、key_sidを読む。
  * value_etcを読む
    * このエントリタプの場合、value部がないので、wite_versionのepoch_number, minor_write_versionのみ読む
    * value_etcのサイズに16byteを指定して、value_etcを読む。

* エントリタイプ
  * marker_begin
  * marker_end
  * marker_durable
  * marker_invalidated_begin
* 処理
  * epoch_idを読む(64bitLE)



### 注意点

* key_sid, value_etcは、Little Endianで書かれている。使う前にエンディアン変換が必要。
* write系メソッドでkey_sid, value_etcを書き込む場合は、Little Endianを前提としていて、エンディアン変換を行っていない。
* 基本的に書き込むとときは、keyとstorrage_idを書き込み、読むときにはそれをkey_sidとして読む。呼んだデータをそのまま各用途用に、key_sidをパラメータにとるwriteメソッドも用意されている。
* valueについても同様に、value, write_versionを書き込み、読むときには、value_etcとして読む。呼んだデータをそのまま各用途用に、value_etcをパラメータにとるwriteメソッドも用意されている。
* key_sid, value_etcはエンディアン変換されずに、そのままentryクラスに保持し、storage_idや、write_versionが参照されるときに、エンディアン変換する。

### BLOBリスト付きのエントリの対応方法

* 案1
  * BLOBリスト付きのエントリ用に新たにエントリタイプを追加する。
  * メリット
    * 過去データをそのまま読める。
* 案2
  * エントリtypeは、normal_entryとし、value部にBLOBリストを含める。
  * BLOBリストの有無や、BLOBリストのサイズの情報が必要なので、normal_entryの構造に変更が必要。
  * 現在拡張用の予約されている領域などがないとで、過去データとの互換性がなくなる。
  

  結論: 案1を採用する。

### 方針

#### log_entryクラスの変更

* エントリタイプの追加
  * BLOBリスト付きのエントリ用のエントリタイプを追加する。
* プライベートフィールドのBLOBリストを追加する。
* read/write用のメソッドの追加
  * BLOBリスト付きのエントリのREAD/WRITE機能を追加する。
    * writeメソッド2種類
    * Readメソッドは、switch〜caseのエントリを追加する。
    * wirteメソッドのswitch〜caseに、BLOBリスト付きのエントリを追加する。

#### log_channelクラスの変更

* log_entryクラスのBLOBリスト付きのwirteメソッドを呼ぶようにする。


#### log_entry::entry_type::normal_entry

コード内で、log_entry::entry_type::normal_entry を参照している箇所をリストアアプし、
BLOBリスト付きのエントリに対応する。

次のケースを想定しているが、他にもあるかもしれない。

* 特に対応不要
* normal_entry に対する処理を、BLOBリスト付きのエントリに対応するように変更する。
* normal_entryに対する処理の他に、BLOBリスト付きのエントリに対する処理を追加する。



