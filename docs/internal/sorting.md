# sorting

`||` で連結を表す


`storage_id || key` に対してその中で wv が最大のものを採用し、 その value を得たい

## PWAL

log_entry :
  [1]type(0x01) [4:LE]key_length [4:LE]value_length [8]storage_id [key_length]key [8:LE]major_wv [8:LE]minor_wv [value_length]value
  or
  [1]type(0x05) [4:LE]key_length [4:LE]value_length [8]storage_id [8:LE]major_wv [8:LE]minor_wv
  or
  [1]type(0x0a) [4:LE]key_length [4:LE]value_length [8]storage_id [key_length]key [8:LE]major_wv [8:LE]minor_wv [value_length]value [4:LE]blobid_length [8*blobid_length]blob_ids


## RMW

DBkey : [8]storage_id [*]key
DBval :
  [1]type(0x01) [8:LE]major_wv [8:LE]minor_wv [*]value
  or
  [1]type(0x05) [8:LE]major_wv [8:LE]minor_wv
  or
  [1]type(0x0a) [8]vwv_length [8:LE]major_wv [8:LE]minor_wv [vwv_length-16]value [*]blob_ids


foreach data in pwals
    let dbkey = data.storage_id || data.key
    let dbval = db.get(dbkey)
    if dbval <> null then
        if dbval.wv < data.wv then
            db.put(dbkey, (data.wv || data.value))
        else
            skip;
    else
        db.put(dbkey, (data.wv || data.value))


問題点:
* RMW をしているが atomic でないので MT-safe でない
* SST ベース KVS では GET が遅い

## PUT_ONLY

storage_id || key が同じであって wv が異なる物も 別エントリとして保存しておく. 取り出す際に wv が最大のものを取り出す.
DBKey を単純に文字列結合すると memcmp order でなくなるので, 専用の Comparator を用意して比較している. (LevelDB, RocksDB 両者にある機能)

DBkey : [8:BE]major_wv [8:BE]minor_wv [8]storage_id [*]key
DBval :
  [1]type(0x01) [*]value
  or
  [1]type(0x05)
  or
  [1]type(0x0a) [8]value_length [value_length]value [*]blob_ids

foreach data in pwals
    let dbkey = data.storage_id || data.key || data.wv
    let dbval = data.value
    db.put(dbkey, dbval)

問題点:
* `storage_id || key` が同じものが複数あった場合に, 後で消されるものが無駄に KVS に保存される

## MERGE

RocksDB の機能 (Debian/Ubuntu 標準パッケージでは 6.11.4-3 まではリンクエラーで使えない, 6.25.3-1 からは使える)

最初のものの RMW を atomic に実行できるのでそれで済ませるというもの

