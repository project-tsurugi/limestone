# limestone-qd 開発用 README

## このリポジトリについて

- このリポジトリは、OSS プロジェクト **limestone** をベースにした **QD プロジェクト向け private リポジトリ** です。
- GitHub 上の OSS リポジトリ（`project-tsurugi/limestone`）で開発されたコードを取り込みつつ、
  RDMA対応の開発・統合テストをここで行います。
- 機密情報保護のため、RDMA 対応関連の情報や改修はこの private リポジトリ内でのみ管理します。

初回は Backlog から clone し、`upstream` として GitHub の OSS リポジトリを追加してください（`git remote add upstream git@github.com:project-tsurugi/limestone.git`）。詳細は「リモートとブランチ構成」「OSS 更新取り込みフロー」を参照してください。

## RDMA 共通ライブラリ依存について

このリポジトリで開発される limestone-qd には、RDMA を利用するための共通ライブラリ `rdma_comm` への依存があります。

- `rdma_comm` は RDMA 通信用の共通ライブラリであり、別リポジトリ（`rdma-comm-lib`）として管理されています。
- ビルド方法やインストール手順は、`rdma-comm-lib` 側の README を参照してください。

ビルド手順は基本的に OSS 版 limestone の README.md に準じますが、`rdma_comm` 依存があるため、次の点が追加で必要になります。

- 事前に `rdma-comm-lib` をビルドし、ライブラリおよびヘッダファイルを任意のプレフィックス（例: `$HOME/opt` や `/opt/rdma_comm`）にインストールしておくこと。
- CMake で limestone-qd を構成する際、`CMAKE_PREFIX_PATH` に `rdma_comm` をインストールしたディレクトリを含めること。
  - 例: `-DCMAKE_PREFIX_PATH=$HOME/opt`
- limestone-qd の CMakeLists.txt では `find_package(rdma_comm REQUIRED)` を行っているため、`rdma_comm` が見つからない環境では CMake の構成に失敗します。

補足（開発者向け）:

- limestone コアライブラリは `rdma_comm` に対して `PRIVATE` リンクを行っており、limestone の公開 API から `rdma_comm` の型や関数を直接露出しない設計としています。

## リモートとブランチ構成

リモート:

- `origin`  
  - Backlog 上の private リポジトリ：`n-tech@n-tech.git.backlog.com:/QD_PRJ/limestone-qd.git`
- `upstream`  
  - GitHub 上の OSS リポジトリ：`git@github.com:project-tsurugi/limestone.git`

主要ブランチの役割:

- `upstream/master` … OSS 本家の開発ブランチ
- `oss_master` … OSS 追従専用ブランチ  
  - `upstream/master` の内容を **fast-forward マージのみ** で取り込む  
  - 原則として手動コミットは行わない（後述のガード推奨）
- `master` … QD プロジェクト用メインブランチ  
  - QD 向けの開発・統合テストは、このブランチから派生したブランチで行う

## OSS 更新取り込みフロー

OSS 側 `master` の更新を取り込む標準手順です。

### 1. upstream/master → oss_master（OSS 追従）

```bash
# OSS 追従専用ブランチへ
git switch oss_master

# OSS 側の更新を取得
git fetch upstream

# fast-forward のみで取り込み
git merge --ff-only upstream/master

# Backlog 側へ反映
git push origin oss_master
```

ポイント:

- OSS 更新取り込みには `git pull` は使わず、必ず `git fetch upstream` → `git merge --ff-only upstream/master` の形を取ります。
- `oss_master` は常に「OSS の状態」を表すブランチとして維持します。

### 2. oss_master → master（QD プロジェクトへの反映）

OSS 追従後、その内容を QD メインブランチ `master` に反映します。

```bash
git switch master
git pull --ff-only origin master   # 念のため最新化

# 可能であれば fast-forward で取り込む
git merge --ff-only oss_master

git push origin master
```


## oss_master のガードと注意事項

`oss_master` は「OSS の状態を反映する」ためのブランチであり、QD 専用開発のコミットは載せません。

### やってよいこと

- `oss_master` 上での以下の操作:
  - `git fetch upstream`
  - `git merge --ff-only upstream/master`
  - `git push origin oss_master`
  - 差分確認（`git log`, `git diff` など）

### やってはいけないこと

- `oss_master` 上での手動 `git commit`
- `oss_master` 上での `git merge`（`--ff-only` なし）や `git rebase`
- QD 向けの修正作業（コード変更・設定変更など）

### ローカルガード設定（推奨）

各自のローカルで、次の設定を入れておくと安全です。

1. `oss_master` の merge を fast-forward のみに制限:

   ```bash
   git config branch.oss_master.mergeOptions "--ff-only"
   ```

2. `oss_master` 上での手動コミットを禁止する `pre-commit` フック（任意）:

   ```bash
   cd .git/hooks

   cat > pre-commit << 'EOF'
   #!/bin/sh

   branch_name="$(git rev-parse --abbrev-ref HEAD)"

   if [ "$branch_name" = "oss_master" ] ; then
       echo "ERROR: Branch 'oss_master' is read-only."
       echo "       このブランチでは手動の git commit は禁止です。"
       echo "       upstream/master の更新は"
       echo "       'git fetch upstream' と"
       echo "       'git merge --ff-only upstream/master'"
       echo "       だけで取り込んでください。"
       exit 1
   fi

   exit 0
   EOF

   chmod +x pre-commit
   ```

※ このフックは各自のローカルにのみ有効です。他の担当者にも適用したい場合は、この README を案内してください。


## 情報取り扱い上の注意

- このリポジトリには、協業先との契約に基づく情報や、外部に公開できない情報が含まれる可能性があります。
- 公開 OSS リポジトリ（`upstream` 側）に戻すコードや情報については、次の点に注意してください:
  - 顧客名や構成図など、利用者や環境を特定できる情報を含めない
  - 案件特有の情報を含めない
  - 協業先固有の契約条件・運用ルールを直接記載しない
- 公開可否に迷う場合は、必ずチーム内で相談し、「公開して問題ない形」に整理してから `upstream` 側への PR を検討してください。


## よく使うコマンド例

```bash
# リモート一覧
git remote -v

# ブランチ一覧（リモート含む）
git branch -a

# OSS 更新の取り込み（upstream → oss_master）
git switch oss_master
git fetch upstream
git merge --ff-only upstream/master
git push origin oss_master

# QD メインへの反映（oss_master → master）
git switch master
git pull --ff-only origin master
git merge --ff-only oss_master
git push origin master
```
