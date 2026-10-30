# it-tools (integration test tools)

limestone を tsurugidb に組み込んで行う結合テストで使う、クライアント側のツールを置きます。

- CMake のビルド・インストール・CI の対象には含めません。
- 各ツールは独立したプロジェクトで、ビルド方法と使い方はそれぞれの README (スクリプト 1 本のツールはスクリプトの冒頭) に記載します。

| ディレクトリ | 内容 |
| --- | --- |
| [`blob-gc-workload/`](./blob-gc-workload/) | オンラインコンパクション時の BLOB ファイル GC の動作確認用ワークロード (tsubakuro、Java) |
| [`blob-id-reuse/`](./blob-id-reuse/) | 参照されなくなった BLOB がある状態で再起動したときの、BLOB ID の再利用と起動時の BLOB ファイル GC をケースごとに確かめるスクリプト (tgctl・tgsql・tglogutil)。使い方は `run-cases.sh` の冒頭を参照 |
