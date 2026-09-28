# blob-gc-workload

オンラインコンパクション時の BLOB ファイル GC の動作確認用ワークロード。tsurugidb に
組み込んだ limestone に対して、tsubakuro (Maven Central) 経由で BLOB を書き込み・読み戻す
小さな Java プログラム。

BLOB 列を持つ表 `blob_gc_test` に INSERT / UPDATE / DELETE を繰り返して生存 BLOB と
ゴミ BLOB を作り (`churn`)、全行の BLOB を読み戻して内容を検証する (`verify`)。
BLOB の内容は (k, gen, sz) から決定的に生成するので、検証に外部の状態は要らない。
GC が生存 BLOB を消していれば `verify` がその行で `BLOB file does not exist` を報告する。

## ビルド

```bash
cd it-tools/blob-gc-workload
./gradlew installDist
```

`installDist` はシステムには何もインストールせず、このディレクトリ配下の
`build/install/blob-gc-workload/` に実行環境一式を展開する。

| パス | 内容 |
| --- | --- |
| `build/install/blob-gc-workload/bin/blob-gc-workload` | 実行スクリプト (Linux / macOS。Windows 用は `.bat`) |
| `build/install/blob-gc-workload/lib/` | 本プログラムと依存 jar (tsubakuro、slf4j) |

`build/` はビルドのたびに作り直され、git の管理外 (`.gitignore`)。
tsubakuro のバージョンは `-PtsubakuroVersion=...` で変えられる (既定 1.16.0)。

## 使い方

```bash
BIN=build/install/blob-gc-workload/bin/blob-gc-workload
$BIN create --url ipc:tsurugi          # 表を作り直す (DROP IF EXISTS + CREATE)
$BIN churn  --url ipc:tsurugi          # INSERT / UPDATE / DELETE の繰り返し
$BIN verify --url ipc:tsurugi          # 全行の BLOB を読み戻して検証 (不一致があれば終了コード 1)
$BIN count  --url ipc:tsurugi          # 行数
```

`churn` のオプション (既定値):

| オプション | 既定 | 意味 |
| --- | --- | --- |
| `--rows N` | 300 | キー範囲 (0 .. N-1) |
| `--iterations N` | 20 | 繰り返し回数。各回で、無い行は INSERT、`k % 7 == 0` の行は DELETE、`k % 3 == 0` の行は UPDATE |
| `--size BYTES` | 16384 | BLOB 1 個のサイズ |
| `--tmpdir DIR` | /tmp/blob-gc-workload | BLOB を渡すための一時ファイル置き場 (commit 後に削除) |
| `--hold-ms MS` | 1500 | BLOB を登録した文の実行後、commit までの待ち時間 |
| `--hold-every N` | 25 | 何トランザクションに 1 回 hold するか (0 で無効) |

1 文 1 トランザクション (OCC)。`hold` は「blob_pool に登録済みで、まだ add_entry
されていない」時間を意図的に広げるためのもので、コンパクション (blob GC) がその窓に
重なることを狙う。オンラインコンパクションは limestone のログディレクトリに
`ctrl/start_compaction` を置くと起動するので、別ターミナルで数秒おきに置き続けながら
`churn` を実行する。

接続は認証なしで、BLOB の転送方式は特権モード (`withBlobTransfer(BlobTransferType.PRIVILEGED)`、
ファイルパス渡し) を明示する。tsubakuro 1.16.0 の既定 (`DEFAULT`) は候補を RELAY、
DOES_NOT_USE の順に送るため、サーバの BLOB relay サービス (gRPC、既定で無効) が無いと
「BLOB functionality is unavailable in this session」になる。特権モードは IPC 接続で
`ipc_endpoint` の `allow_blob_privileged` (既定 true) が許可していれば使え、tsurugidb と
同じユーザで同じホストから実行する必要がある。
