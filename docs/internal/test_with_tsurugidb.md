 # Tsurugidbの更新

 ## 黒澤さんのSlackのポスト
 
 
 tsurugidb更新の手順について簡単なガイドです。

tsurugi全体でリリースに使用するソースコードのバージョンはtsurugidbレポジトリ ( https://github.com/project-tsurugi/tsurugidb )がまとめており、tsurugiの各リリースに反映させるためには、各コンポーネントのmasterに反映するだけでなく、tsurugidbのmasterが参照するコミットを更新する必要があります。以下その更新手順です。
 
  - コンポーネント(jogasakiやlimestoneなど)の方のmasterに変更をpushする(レビューが必要な場合はPR経由、些細なものは直接commitをpushなど)
    - この段階のCIでは、依存コンポーネントがある場合はそのmasterを使用してビルドするので、依存コンポーネントの側の変更もある場合はそちらを先にmaster更新する必要がある
    - 例えば、tateyamaの変更に依存したjogasakiの修正がある場合は、tateyamaをmasterに反映してからjogasakiをmasterに反映する
  - tsurugidbのmasterを更新して、tsurugidbが参照する各コンポーネントのコミットを更新してmasterにpushします
    - tsurugidbのレポジトリが手元になければcloneしてsubmoduleを更新する
      - git clone git@github.com:project-tsurugi/tsurugidb.git
      - cd tsurugidb
      - git submodule update --init --recursive
    - 下記はjogasakiのmaster最新版を参照するようにtsurugidb/jogasakiを更新する例です
      - cd tsurugidb
      - git submodule update --remote jogasaki
      - git add .
      - git commit -m "xxx" (コミットメッセージの書き方は下記を参照)
      - git push
- commitメッセージの書き方
tsurugidbへのコミットからrelease noteの記述を自動生成しているため、tsurugidbにはコミットメッセージについてのルールがあります。
おおまかには、`fix(jogasaki): xxxxxxxx` のようにコンポーネント名と修正内容をプレフィックスとしてつける、既存のクライアント・サーバーとの互換性についてbreaking changeがある場合はcommitメッセージ本文(ヘッダでないほう)にBREAKING CHANGEというワードをいれる。外部に公開されるので、外部ユーザーが読んである程度理解できるレベルにする。といったルールがあります。
プレフィックスやBREAKING CHANGEについては下記を参照
https://www.conventionalcommits.org/en/v1.0.0/
ルール制定時に行った議論の詳細はこのあたりにあります：
https://github.com/project-tsurugi/tsurugi-issues/discussions/397


## 2024/08/20 ログ

### LOG-0.6入のTsurugiをセットアプする

* tsurugidbリポジトリの操作

```
cd ~/git
git clone  git@github.com:project-tsurugi/tsurugidb.git
cd tsurugidb
git submodule update --init --recursive
```
* この状態だとlimesotneの一部のブランチが見えないので見えるようにする

```
cd ~/git/tsurugidb/limestone
git fetch --all
```

* tsurugidbの作業用ブランチを作成する => 以降、このブランチで作業する。

```
cd ~/git/tsurugidb/
git checkout -b wip/log-0.6
```

* limestoneのブランチをwip/log-0.6に変更する。

```
cd limestone/
git switch wip/log-0.6 
```

* tsurugiのインストール

```
./install.sh --symbolic --prefix=$HOME/tsurugi
```

### 動作確認

* phone-bill-benchmarkを動かしてい見る

```
cd ~/git/phone-bill-benchmark/scripts/
git pull
./tinit.sh 
./multiple_execute.sh config-cb-tiny/03-LTX-T01 
./tstop.sh 
```

* とりえあえず、エラーが起きずに動いた。

```
umegane@ubuntu22:~/git/phone-bill-benchmark/scripts$ ls -l /home/umegane/tsurugi/tsurugi/var/data/log
合計 11312
-rw-rw-r-- 1 umegane umegane      67  8月 20 17:46 compaction_catalog
drwxrwxr-x 2 umegane umegane    4096  8月 20 17:46 ctrl
drwxrwxr-x 2 umegane umegane    4096  8月 20 17:46 data
-rw-rw-r-- 1 umegane umegane     153  8月 20 17:47 epoch
-rw-rw-r-- 1 umegane umegane      67  8月 20 17:46 limestone-manifest.json
-rw-rw-r-- 1 umegane umegane 6289776  8月 20 17:47 pwal_0000
-rw-rw-r-- 1 umegane umegane 2634236  8月 20 17:47 pwal_0001
-rw-rw-r-- 1 umegane umegane 2630009  8月 20 17:47 pwal_0002
```
* ログディレクトリの確認、とりあえず問題なさそう。

* このままだとローテーションの確認ができないので、テスト用に1分に1回ローテーションするようにコードを修正する。
