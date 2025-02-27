# バックアップとリストアに関するメモ

## バックアップファイルをフラットに持つのか、ディレクトリ構造を維持するのか

### 発端となった議論 

[SLACKでの議論](https://nautilus-rd.slack.com/archives/GB4QT920L/p1740620186533359)より

limestoneのバックアップ機能、UTまで通りましたが、tgctlコマンドで動かすとNGでした。
```
umegane@ubuntu22:~/logs$ find $TSURUGI_HOME/var/data/log
/home/umegane/tsurugi/tsurugi/var/data/log
/home/umegane/tsurugi/tsurugi/var/data/log/ctrl
/home/umegane/tsurugi/tsurugi/var/data/log/epoch
/home/umegane/tsurugi/tsurugi/var/data/log/pwal_0000
/home/umegane/tsurugi/tsurugi/var/data/log/blob
/home/umegane/tsurugi/tsurugi/var/data/log/blob/dir_01
/home/umegane/tsurugi/tsurugi/var/data/log/blob/dir_01/0000000000000001.blob
/home/umegane/tsurugi/tsurugi/var/data/log/data
/home/umegane/tsurugi/tsurugi/var/data/log/data/snapshot
/home/umegane/tsurugi/tsurugi/var/data/log/compaction_catalog
/home/umegane/tsurugi/tsurugi/var/data/log/pwal_0001
/home/umegane/tsurugi/tsurugi/var/data/log/limestone-manifest.json
umegane@ubuntu22:~/logs$ tgctl status
Tsurugi OLTP database is RUNNING
umegane@ubuntu22:~/logs$ tgctl backup create ~/work/bk-online/ --verbose
umegane@ubuntu22:~/logs$ tgctl shutdown
.
successfully shutdown tsurugidb.
umegane@ubuntu22:~/logs$ tgctl backup create ~/work/bk-offline/ --verbose
umegane@ubuntu22:~/logs$ find ~/work/bk-online/
/home/umegane/work/bk-online/
/home/umegane/work/bk-online/epoch
/home/umegane/work/bk-online/pwal_0000
/home/umegane/work/bk-online/0000000000000001.blob
/home/umegane/work/bk-online/compaction_catalog
/home/umegane/work/bk-online/pwal_0001
/home/umegane/work/bk-online/limestone-manifest.json
umegane@ubuntu22:~/logs$ find ~/work/bk-offline/
/home/umegane/work/bk-offline/
/home/umegane/work/bk-offline/epoch
/home/umegane/work/bk-offline/pwal_0000
/home/umegane/work/bk-offline/0000000000000001.blob
/home/umegane/work/bk-offline/compaction_catalog
/home/umegane/work/bk-offline/pwal_0001
/home/umegane/work/bk-offline/limestone-manifest.json
```
* blobファイルは、logディレクトリにあるblobディレクトリの配下に作成されるのですが、
backupするとディレクトリ構造が維持されず、blobファイルが全てバックアップ先のディレクトリ
直下に集まってしまいます。
* 今までは、logディレクトリ直下にしかバックアップ対象のディレクトリが存在しなかったので
問題なかったのだと思います。

**これに対するコメント**

* ただこれ、バックアップの仕様通りなのでちょっと考える必要がありますね。
たぶんbelayerもだめそう

**とりあえずの方針**

* 現状:
  * フルバックアップは、バックアップ作成時のファイルリストのうち、単純名だけを記憶してファイルを複製しておけばよい (ディレクトリ構造は失われる)
  * 増分バックアップは、ディレクトリ構造ごと維持するようにしているのでこの問題はなし
* 当面の対応:
  * リストア側で拡張子等からディレクトリ構造を復元する
Belayer 本体のバックアップ機能も同様の仕様で作っているはずなので、仕様変更は行わない (breaking change となる)
* 以降の対応:
  * ディレクトリ構造を維持するように、バックアップの仕様 (要求) を変更する
  * 古いバックアップは、limestone-manifest.json ファイルをみて正しくリストアする

#### その他のコメント

* BLOBファイルのディレクトリを分けているのは、BLOBファイルが大量に存在するときの、ファイルオペレーションに問題を来さないため
  * 主に人間の操作を想定している
  * プログラムからファイルにアクセスする場合も問題になりえる。
    * 単純にファイル操作が遅くなる
    * 何らかの制限にひっかかり、操作できなくなる。
  * 単にblobディレクトリを作るだけでなく、blobディレクトリの下にサブディレクトリを作成し、そのサブディレクトリに保存している。
* 現状のしようだと、上の項目の問題があると考えるが、大量にBLOBファイルが存在する場合の問題なので、当面は現状通りで問題ないと考える。

## その他の疑問点

* backup/restoreは、それぞれ2つずつのI/Fをもつが、どのように使い分けているのか分からない。
  * 使っていないなら削除したい。
  * 使ってイエモ、2つのI/Fが必要でないのなら、片方だけにしたい。

1. begin_backup
```cpp
/**
 * @brief start backup operation
 * @detail a backup object is created, which contains a list of log files.
 * @exception limestone_io_exception Thrown if an I/O error occurs.
 * @note Currently, this function does not throw an exception but logs the error and aborts the process.
 *       However, throwing an exception is the intended behavior, and this will be restored in future versions.
 *       Therefore, callers of this API must handle the exception properly as per the original design.
 * @return a reference to the backup object.
 */
backup& begin_backup();
```

2. begin_backup (prusik era)
```cpp
// backup (prusik era)
/**
 * @brief start backup operation
 * @detail a backup_detail object is created, which contains a list of log entry.
 * @exception limestone_io_exception Thrown if an I/O error occurs.
 * @note Currently, this function does not throw an exception but logs the error and aborts the process.
 *       However, throwing an exception is the intended behavior, and this will be restored in future versions.
 *       Therefore, callers of this API must handle the exception properly as per the original design.
 * @return a reference to the backup_detail object.
 */
std::unique_ptr<backup_detail> begin_backup(backup_type btype);
```    

3. restore
```cpp
/**
 * @brief restore log files, etc. located at from directory
 * @details log file, etc. stored in from directroy are to be copied to log directory.
 * @param from the location of the log files backuped
 * @attention this function is not thread-safe.
 * @return status indicating whether the process ends successfully or not
 */
status restore(std::string_view from, bool keep_backup) const noexcept;
```

4. restore
```cpp
// restore (prusik era)
status restore(std::string_view from, std::vector<file_set_entry>& entries) noexcept;
```

* backup_dtail::entry, file_set_entryともに、src,dstのpathを持っているので、
  ディレクトリ構造を保ってバックアップするのだと思ってしまいました。
  * 現使用だと、dstのpathは無視し、ファイル名のみ使うのが正しいのですね。
  * dstがpathを持っているので、dstのpathの解釈を変えるだけで、
    ディレクトリ構造を保ったバックアップ、リストアに対応できる。