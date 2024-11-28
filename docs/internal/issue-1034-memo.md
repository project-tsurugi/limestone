# Issue #1034 分析メモ

## Issueから

### クライアントのログ

10:47:10.148 にTXのコミット完了が記録されている。

```
10:47:10.148 [pool-2-thread-12] DEBUG c.t.b.p.a.b.CalculationTask-cmt - Transaction completed, tid = TID-000000050000000b, txOption = LTX{label=BATCH_MAIN, writePreserve=[history, billing]}, key = 00000000147, update/insert records = 132
10:47:10.148 [pool-2-thread-8] DEBUG c.t.b.p.a.b.CalculationTask-cmt - Transaction completed, tid = TID-0000000200000013, txOption = LTX{label=BATCH_MAIN, writePreserve=[history, billing]}, key = 00000000146, update/insert records = 139
```

### tsurugidbの停止時刻

systemdのログから、10:47:10.321249 にtsurugidbがSIGKILLで終了したことを検知

### epochファイルとPWALファイルのタイムスタンプ

```
-rw-r--r-- 1 umegane umegane 3160157 2024-11-22 10:47:09.735010923 +0900 pwal_0004
-rw-r--r-- 1 umegane umegane 3284754 2024-11-22 10:47:09.962972421 +0900 pwal_0001
-rw-r--r-- 1 umegane umegane    1611 2024-11-22 10:47:10.134944734 +0900 epoch
-rw-r--r-- 1 umegane umegane 3007382 2024-11-22 10:48:11.115999194 +0900 pwal_0005
-rw-r--r-- 1 umegane umegane 3296144 2024-11-22 10:48:11.223999192 +0900 pwal_0002
```

epochファイルのタイムスタンプは、クライアントがTXのコミット完了を記録した時刻よりわずかに古い
epochファイル更新後に更新されているPWALファイルが2つある。

## tglogutil inspect

```
umegane@ubuntu22:~/work$ ~/tsurugi/tsurugi/bin/tglogutil  inspect data/
W1126 15:39:42.223834 188472 dblogutil.cpp:331] WARNING: subcommand 'inspect' is under development
dblogdir: "data/"
persistent-format-version: 1
durable-epoch: 13341
E1126 15:39:42.258160 188473 dblogutil.cpp:87] 8
E1126 15:39:42.340119 188473 dblogutil.cpp:87] 8
E1126 15:39:42.340130 188473 dblogutil.cpp:87] 8
E1126 15:39:42.340134 188473 dblogutil.cpp:87] 8
E1126 15:39:42.340137 188473 dblogutil.cpp:87] 8
E1126 15:39:42.340139 188473 dblogutil.cpp:87] 8
E1126 15:39:42.340143 188473 dblogutil.cpp:87] 9
E1126 15:39:42.340147 188473 dblogutil.cpp:87] 9
E1126 15:39:42.340148 188473 dblogutil.cpp:87] 9
E1126 15:39:42.340152 188473 dblogutil.cpp:87] 9
E1126 15:39:42.340154 188473 dblogutil.cpp:87] 9
E1126 15:39:42.340157 188473 dblogutil.cpp:87] 8
E1126 15:39:42.340159 188473 dblogutil.cpp:87] 8
E1126 15:39:42.340162 188473 dblogutil.cpp:87] 8
E1126 15:39:42.340164 188473 dblogutil.cpp:87] 8
E1126 15:39:42.340166 188473 dblogutil.cpp:87] 8
max-appeared-epoch: 13343
count-durable-wal-entries: 959076
status: OK
```
durable-epoch: 13341
max-appeared-epoch: 13343


## walファイルの分析

walのテキスト化ツールで、テキスト化したwalをgrepで検索

```
umegane@ubuntu22:~/git/limestone/test_data$ grep -n 'Entry Type' out-*.txt | grep -v normal_entry, | grep 'Epoch ID: 133'
out-pwal_0001.txt:308253:Entry Type: marker_begin, Storage ID: 0, Epoch ID: 13315
out-pwal_0002.txt:308931:Entry Type: marker_invalidated_begin, Storage ID: 0, Epoch ID: 13342
out-pwal_0005.txt:282060:Entry Type: marker_invalidated_begin, Storage ID: 0, Epoch ID: 13343
```
durable-epoch: 13341より大きなEpoch IDのエントリを持つのは、pwal_0002とpwal_0005のみ


pwal_0002のEpoch ID: 13342のエントリを確認
=> key = 00000000146 のデータのか書き込み


pwal_0005のEpoch ID: 13343のエントリを確認
=> key = 00000000147 のデータのか書き込み


## pwalのタイムスタンプについて

いままではTsurugi起動後のPWALファイルをみていたので、Tsurugi起動前のスナップショットを見てみる。

```
umegane@sk202:/work/poff-test/data$ !1956
ls -ltr --full-time | tail -n 10
-rw-r--r-- 1 umegane umegane 2905421 2024-11-22 10:47:08.891154764 +0900 pwal_0009
-rw-r--r-- 1 umegane umegane 2875667 2024-11-22 10:47:08.995136031 +0900 pwal_0010
-rw-r--r-- 1 umegane umegane 2703935 2024-11-22 10:47:09.159108193 +0900 pwal_0011
-rw-r--r-- 1 umegane umegane 3195785 2024-11-22 10:47:09.575037943 +0900 pwal_0003
-rw-r--r-- 1 umegane umegane 3419116 2024-11-22 10:47:09.731011600 +0900 pwal_0000
-rw-r--r-- 1 umegane umegane 3160157 2024-11-22 10:47:09.735010923 +0900 pwal_0004
-rw-r--r-- 1 umegane umegane 3284754 2024-11-22 10:47:09.962972421 +0900 pwal_0001
-rw-r--r-- 1 umegane umegane 3296144 2024-11-22 10:47:10.082952966 +0900 pwal_0002
-rw-r--r-- 1 umegane umegane 3007382 2024-11-22 10:47:10.094951067 +0900 pwal_0005
-rw-r--r-- 1 umegane umegane    1611 2024-11-22 10:47:10.134944734 +0900 epoch
```

pwal_0002, pwal_0005もpeochファイルより古いが、ほぼ同時刻。
以前見ていたのは、起動時に書き換わったPWALファイルかも



## 2024-11-27

トレースログを入れて再現させてみる

```
20241126-205011.log:kill-test.sh failed with error code 13
20241127-000544.log:kill-test.sh failed with error code 13
20241127-005046.log:kill-test.sh failed with error code 13
20241127-031608.log:kill-test.sh failed with error code 13
20241127-045800.log:kill-test.sh failed with error code 13
20241127-053501.log:kill-test.sh failed with error code 13
20241127-055644.log:kill-test.sh failed with error code 13
20241127-095136.log:kill-test.sh failed with error code 13
```

再現したみたい。`20241127-095136.log`を調べてみる。


```
+ VBoxManage snapshot sk202 take poff-20241127-095136
0%...10%...20%...30%...40%...50%...60%...70%...80%...90%...100%
Snapshot taken. UUID: 727b376b-edaf-4a85-8f75-0e5604004a37
+ /home/kill/tsurugi-durability-test/scripts/poff/boot.sh
Waiting for VM "sk202" to power on...
VM "sk202" has been successfully started.
Waiting for SSH login...
Waiting for SSH login...
Waiting for SSH login...
Waiting for SSH login...
Waiting for SSH login...
Waiting for SSH login...
VM is ready for SSH login.
+ ssh -t -o StrictHostKeyChecking=no umegane@sk202 'sudo systemctl start tsurugidb'
Pseudo-terminal will not be allocated because stdin is not a terminal.^M
+ REPAIR=0
+ ssh -o StrictHostKeyChecking=no umegane@sk202 'sudo systemctl is-active --quiet tsurugidb'
+ ssh -o StrictHostKeyChecking=no umegane@sk202 '/home/umegane/tsurugi-durability-test/scripts/confirm_db_integrity.sh poff-test'
Database inconsistency detected.
+ RET=1
+ '[' 1 -ne 0 ']'
+ '[' 0 -ne 0 ']'
+ VBoxManage snapshot sk202 take error-13-20241127-095136
0%...10%...20%...30%...40%...50%...60%...70%...80%...90%...100%
Snapshot taken. UUID: e762b4a2-3e1b-4e9d-b522-05f27379ae17
```

やはりDBの整合性チェックでエラーが起きている。
スナップショットは次の2つ

poff-20241127-095136
error-13-20241127-095136

スナップショットを調べる

```
kill@sk043:~/tsurugi-durability-test/scripts/poff$ vboxmanage snapshot sk202 restore error-13-20241127-095136
Restoring snapshot 'error-13-20241127-095136' (e762b4a2-3e1b-4e9d-b522-05f27379ae17)
0%...10%...20%...30%...40%...50%...60%...70%...80%...90%...100%
```

```
11月 27 09:51:36 sk202 systemd[1]: Started tsurugidb.
11月 27 09:52:16 sk202 systemd[1]: tsurugidb.service: Main process exited, code=killed, status=9/KILL
11月 27 09:52:16 sk202 systemd[1]: tsurugidb.service: Failed with result 'signal'.
11月 27 09:52:16 sk202 systemd[1]: tsurugidb.service: Consumed 3min 35.697s CPU time.
-- Boot 65ddcfc82a8c4949b9501baeafed5501 --
11月 27 09:53:10 sk202 systemd[1]: Starting tsurugidb...
11月 27 09:53:35 sk202 tgctl[921]: successfully launched tsurugidb.
11月 27 09:53:35 sk202 systemd[1]: Started tsurugidb.
```

tsurugiが、09:52:16 にKILLされていることを確認。

```
09:53:52.317 [main] ERROR c.t.b.p.a.d.ConfirmDbIntegrity - Database inconsistency detected.
```

KILL後の起動音あと、DB不整合を検出

key = 00000000217 のTXが更新前の状態

```
09:52:15.671 [pool-2-thread-5] DEBUG c.t.b.p.a.b.CalculationTask-cmt - Transaction committing, tid = TID-0000000d00000004, txOption = LTX{label=BATCH_MAIN, writePreserve=[history, billing]}, key = 00000000217
09:52:16.266 [pool-2-thread-5] DEBUG c.t.b.p.a.b.CalculationTask-cmt - Transaction completed, tid = TID-0000000d00000004, txOption = LTX{label=BATCH_MAIN, writePreserve=[history, billing]}, key = 00000000217, update/insert records = 140
```

クライアントのろぐに09:52:16.266 コミット成功のログが出ている。

````
umegane@ubuntu22:~/work/issue1034-2$ ~/tsurugi/tsurugi/bin/tglogutil inspect data
W1127 11:39:23.450129 99022 dblogutil.cpp:331] WARNING: subcommand 'inspect' is under development
dblogdir: "data"
persistent-format-version: 1
durable-epoch: 9561
E1127 11:39:23.486958 99023 dblogutil.cpp:87] 8
E1127 11:39:23.569765 99023 dblogutil.cpp:87] 8
E1127 11:39:23.569778 99023 dblogutil.cpp:87] 8
E1127 11:39:23.569780 99023 dblogutil.cpp:87] 8
E1127 11:39:23.569782 99023 dblogutil.cpp:87] 8
E1127 11:39:23.569783 99023 dblogutil.cpp:87] 8
E1127 11:39:23.569785 99023 dblogutil.cpp:87] 9
E1127 11:39:23.569787 99023 dblogutil.cpp:87] 9
E1127 11:39:23.569788 99023 dblogutil.cpp:87] 9
E1127 11:39:23.569789 99023 dblogutil.cpp:87] 9
E1127 11:39:23.569790 99023 dblogutil.cpp:87] 9
E1127 11:39:23.569792 99023 dblogutil.cpp:87] 8
E1127 11:39:23.569793 99023 dblogutil.cpp:87] 8
E1127 11:39:23.569795 99023 dblogutil.cpp:87] 8
E1127 11:39:23.569797 99023 dblogutil.cpp:87] 8
E1127 11:39:23.569798 99023 dblogutil.cpp:87] 8
max-appeared-epoch: 9562
count-durable-wal-entries: 987101
status: auto-repairable
```

tglogutilの結果から、durable-epoch: 9561で、epoch 9562のエントリがPWALに存在することが分かる。


pwal_0013 にのみepoch 9562のエントリが存在し、このエポックに139エントリが存在し、すべて、
KEY=00000000217....C のエントリだった。

Epochファイルと、PWALファイルのタイムスタンプ

```
-rw-r--r-- 1 umegane umegane 2630009 2024-11-27 09:51:50.592230636 +0900 pwal_0024
-rw-r--r-- 1 umegane umegane 2630009 2024-11-27 09:51:50.744230633 +0900 pwal_0023
-rw-r--r-- 1 umegane umegane 2630009 2024-11-27 09:51:50.788230632 +0900 pwal_0020
-rw-r--r-- 1 umegane umegane 2630009 2024-11-27 09:51:51.320230622 +0900 pwal_0016
-rw-r--r-- 1 umegane umegane 2630009 2024-11-27 09:51:51.512230618 +0900 pwal_0025
-rw-r--r-- 1 umegane umegane 2630009 2024-11-27 09:51:51.644230615 +0900 pwal_0017
-rw-r--r-- 1 umegane umegane 2630009 2024-11-27 09:51:51.996230608 +0900 pwal_0029
-rw-r--r-- 1 umegane umegane 2630009 2024-11-27 09:51:52.344230602 +0900 pwal_0027
-rw-r--r-- 1 umegane umegane 2630009 2024-11-27 09:51:52.440230600 +0900 pwal_0026
-rw-r--r-- 1 umegane umegane 2630009 2024-11-27 09:51:52.488230599 +0900 pwal_0021
-rw-r--r-- 1 umegane umegane 2630009 2024-11-27 09:51:52.528230598 +0900 pwal_0028
-rw-r--r-- 1 umegane umegane 2630009 2024-11-27 09:51:52.612230596 +0900 pwal_0018
-rw-r--r-- 1 umegane umegane 2630009 2024-11-27 09:51:52.744230594 +0900 pwal_0019
-rw-r--r-- 1 umegane umegane 2630009 2024-11-27 09:51:53.344230582 +0900 pwal_0022
-rw-r--r-- 1 umegane umegane 2664836 2024-11-27 09:51:56.804230513 +0900 pwal_0014
-rw-r--r-- 1 umegane umegane 2668574 2024-11-27 09:51:56.896230511 +0900 pwal_0015
-rw-r--r-- 1 umegane umegane 3522303 2024-11-27 09:52:15.552230142 +0900 pwal_0001
-rw-r--r-- 1 umegane umegane 3504722 2024-11-27 09:52:15.564230141 +0900 pwal_0002
-rw-r--r-- 1 umegane umegane 3330704 2024-11-27 09:52:15.656230139 +0900 pwal_0003
-rw-r--r-- 1 umegane umegane 3170837 2024-11-27 09:52:15.740230138 +0900 pwal_0004
-rw-r--r-- 1 umegane umegane 3092372 2024-11-27 09:52:15.752230138 +0900 pwal_0005
-rw-r--r-- 1 umegane umegane 3055259 2024-11-27 09:52:15.796230137 +0900 pwal_0006
-rw-r--r-- 1 umegane umegane 3174341 2024-11-27 09:52:15.820230136 +0900 pwal_0007
-rw-r--r-- 1 umegane umegane 3096644 2024-11-27 09:52:15.852230136 +0900 pwal_0008
-rw-r--r-- 1 umegane umegane 3103853 2024-11-27 09:52:15.884230135 +0900 pwal_0009
-rw-r--r-- 1 umegane umegane 3106790 2024-11-27 09:52:15.964230133 +0900 pwal_0010
-rw-r--r-- 1 umegane umegane 3058079 2024-11-27 09:52:16.052230132 +0900 pwal_0011
-rw-r--r-- 1 umegane umegane 2936243 2024-11-27 09:52:16.172230129 +0900 pwal_0012
-rw-r--r-- 1 umegane umegane 3637807 2024-11-27 09:52:16.172230129 +0900 pwal_0000
-rw-r--r-- 1 umegane umegane 2739029 2024-11-27 09:52:16.220230128 +0900 pwal_0013
-rw-r--r-- 1 umegane umegane    2322 2024-11-27 09:52:16.248230128 +0900 epoch
```

limestoneのトレースログから

* epoch 9562のpersistent callbackは呼ばれていない




```
09:52:15.671 [pool-2-thread-5] DEBUG c.t.b.p.a.b.CalculationTask-cmt - Transaction committing, tid = TID-0000000d00000004, txOption = LTX{label=BATCH_MAIN, writePreserve=[history, billing]}, key = 00000000217
09:52:16.266 [pool-2-thread-5] DEBUG c.t.b.p.a.b.CalculationTask-cmt - Transaction completed, tid = TID-0000000d00000004, txOption = LTX{label=BATCH_MAIN, writePreserve=[history, billing]}, key = 00000000217, update/insert records = 140
```
* クライアントがコミット完了をログに記録する500msほど前に、Epoch 9561 のpersistent callbackが呼ばれている。

```
I1127 09:52:16.253589  1529 datastore.cpp:218] /:limestone:api:datastore:update_min_epoch_id start calling persistent callback to 9561
I1127 09:52:16.253695  1529 datastore.cpp:220] /:limestone:api:datastore:update_min_epoch_id end calling persistent callback to 9561
```

* その直後に、Epoch 9562のセッションが終了している。

```
I1127 09:52:16.253742  1529 log_channel.cpp:96] /:limestone:api:log_channel:end_session end end_session() with current_epoch_id_=18446744073709551615
I1127 09:52:16.263021  1540 datastore.cpp:187] /:limestone:api:datastore:update_min_epoch_id start update epooch file to 9573
```