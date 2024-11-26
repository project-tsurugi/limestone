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



