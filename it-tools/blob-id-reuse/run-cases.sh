#!/usr/bin/env bash
#
# 参照されなくなった BLOB (削除したもの、durable にならなかったもの) がある状態で再起動したときに、その BLOB の
# ID が再利用されるか、そのファイルが起動時の BLOB GC で回収されるかを、ケースごとに確かめる。
# 起動時の BLOB GC の完了を limestone の INFO ログ "blob files garbage collection finished" で待つので、このログを
# 出す版の tsurugidb が必要。
#
# usage: run-cases.sh <tsurugi_home> <work_dir> [case ...]
#
#   tsurugi_home  tsurugidb のインストールディレクトリ (bin/tgctl などがあるところ)
#   work_dir      ケースごとのデータディレクトリを作る場所 (ケースのディレクトリは毎回作り直す)
#   case          実行するケース (省略時は全ケース)。ケースの一覧は usage を参照
#
# ケースごとに、インストールディレクトリの tsurugi.ini を元に log_location・database_name・glog の log_dir を
# 書き換えた設定ファイルを作り、tgctl --conf で起動する。インストールディレクトリの var/data は使わない。

set -euo pipefail

readonly DB_NAME=blob_id_reuse
readonly ALL_CASES=(delete_newest delete_all truncate drop update_null no_compaction online_compaction offline_compaction
                    nondurable)

usage() {
    cat >&2 <<EOF
usage: $0 <tsurugi_home> <work_dir> [case ...]
cases:
  u (ID 1)、t (ID 2〜4) の順に作成して、最も新しい BLOB (ID 4) を含む BLOB を削除する
    delete_newest       t の ID 4 の行を DELETE
    delete_all          t の全行を DELETE
    truncate            t を TRUNCATE
    drop                t を DROP TABLE
    update_null         t の ID 4 の行の BLOB 列を NULL に UPDATE
  t (ID 1〜3) を作成して、ID 3 の行を DELETE する
    no_compaction       再起動前にコンパクションをしない
    online_compaction   再起動前にオンラインコンパクション
    offline_compaction  停止中に tglogutil compaction
  t (ID 1〜2) を作成して停止し、epoch ファイルを退避する。起動して t に BLOB (ID 3) を INSERT し、停止してから
  epoch ファイルを戻す (ID 3 のエントリは pwal にあるが durable でない、クラッシュ直後と同じ状態)
    nondurable
EOF
    exit 2
}

[[ $# -ge 2 ]] || usage
TSURUGI_HOME=$(cd "$1" && pwd)
export TSURUGI_HOME
WORK_DIR=$(mkdir -p "$2" && cd "$2" && pwd)
shift 2
CASES=("$@")
[[ ${#CASES[@]} -gt 0 ]] || CASES=("${ALL_CASES[@]}")

readonly TGCTL=$TSURUGI_HOME/bin/tgctl
readonly TGSQL=$TSURUGI_HOME/bin/tgsql
readonly TGLOGUTIL=$TSURUGI_HOME/bin/tglogutil

for c in "${CASES[@]}"; do
    [[ " ${ALL_CASES[*]} " == *" $c "* ]] || { echo "unknown case: $c" >&2; usage; }
done
if pgrep -x tsurugidb >/dev/null; then
    echo "tsurugidb is already running; stop it first" >&2
    exit 1
fi

# 実行中のケースの状態 (異常終了時の後始末と、各関数が使う)
case_dir=""
conf=""
running=false

cleanup() {
    if $running; then
        echo "aborted: shutting down tsurugidb" >&2
        "$TGCTL" shutdown --conf "$conf" --timeout 60 >>"$case_dir/tgctl.out" 2>&1 || "$TGCTL" kill --conf "$conf" || true
    fi
}
trap cleanup EXIT

fail() {
    echo "FAILED ($1): see $case_dir" >&2
    exit 1
}

prepare_case_dir() {
    case_dir=$WORK_DIR/$1
    rm -rf "$case_dir"
    mkdir -p "$case_dir/log" "$case_dir/glog"
    conf=$case_dir/tsurugi.ini
    sed -e "s|^    log_location=.*\$|    log_location=$case_dir/log|" \
        -e "s|^    #database_name=tsurugi\$|    database_name=$DB_NAME|" \
        -e "s|^    #log_dir=\$|    log_dir=$case_dir/glog|" \
        "$TSURUGI_HOME/var/etc/tsurugi.ini" >"$conf"
    grep -q "^    log_location=$case_dir/log\$" "$conf" || fail "log_location is not set in $conf"
    grep -q "^    database_name=$DB_NAME\$" "$conf" || fail "database_name is not set in $conf"
    grep -q "^    log_dir=$case_dir/glog\$" "$conf" || fail "log_dir is not set in $conf"
}

start_db() {
    running=true  # 起動に失敗してもプロセスが残りうるので、先に立てて cleanup に後始末させる
    "$TGCTL" start --conf "$conf" >>"$case_dir/tgctl.out" 2>&1 || fail "tgctl start"
}

shutdown_db() {
    "$TGCTL" shutdown --conf "$conf" --timeout 60 >>"$case_dir/tgctl.out" 2>&1 || fail "tgctl shutdown"
    running=false
}

# 1 文ずつ commit する (commit_response の既定は STORED なので、commit が返った時点で durable)
run_sql() {
    local name=$1
    shift
    printf '%s\n' "$@" >"$case_dir/$name.sql"
    "$TGSQL" --script -c "ipc:$DB_NAME" --no-auth --auto-commit "$case_dir/$name.sql" >"$case_dir/$name.out" 2>&1 \
        || fail "tgsql $name"
}

# BLOB ファイルを "ID:内容 (16 進)" で 1 行ずつ、ID の昇順に出力する
list_blobs() {
    local blob_dir=$case_dir/log/blob
    [[ -d $blob_dir ]] || return 0
    find "$blob_dir" -name '*.blob' -print0 | while IFS= read -r -d '' f; do
        local name
        name=$(basename "$f" .blob)
        printf '%d:%s\n' "$((16#$name))" "$(od -An -tx1 -v "$f" | tr -d ' \n')"
    done | sort -t: -k1,1n
}

wait_for_log() {
    local pattern=$1 count=$2
    for _ in $(seq 60); do
        if [[ $(grep -c -- "$pattern" "$case_dir/glog/tsurugidb.INFO" 2>/dev/null || true) -ge $count ]]; then
            return 0
        fi
        sleep 1
    done
    fail "timed out waiting for the log: $pattern"
}

# 結果の表の行
results=()

run_case() {
    local c=$1
    local insert_table referenced before_restart=none
    prepare_case_dir "$c"
    echo "=== $c ($case_dir)" >&2

    start_db
    case $c in
    delete_newest | delete_all | truncate | drop | update_null)
        run_sql setup \
            "create table u (pk int primary key, bl blob);" \
            "create table t (pk int primary key, bl blob);" \
            "insert into u values (1, cast(X'01' as blob));" \
            "insert into t values (2, cast(X'02' as blob));" \
            "insert into t values (3, cast(X'03' as blob));" \
            "insert into t values (4, cast(X'04' as blob));"
        insert_table=u
        ;;
    nondurable)
        run_sql setup \
            "create table t (pk int primary key, bl blob);" \
            "insert into t values (1, cast(X'01' as blob));" \
            "insert into t values (2, cast(X'02' as blob));"
        insert_table=t
        ;;
    *)
        run_sql setup \
            "create table t (pk int primary key, bl blob);" \
            "insert into t values (1, cast(X'01' as blob));" \
            "insert into t values (2, cast(X'02' as blob));" \
            "insert into t values (3, cast(X'03' as blob));"
        insert_table=t
        ;;
    esac
    list_blobs >"$case_dir/blobs-1-after-setup.txt"

    case $c in
    delete_newest) run_sql operation "delete from t where pk = 4;"; referenced="1 2 3" ;;
    delete_all) run_sql operation "delete from t;"; referenced="1" ;;
    truncate) run_sql operation "truncate table t;"; referenced="1" ;;
    drop) run_sql operation "drop table t;"; referenced="1" ;;
    update_null) run_sql operation "update t set bl = null where pk = 4;"; referenced="1 2 3" ;;
    no_compaction) run_sql operation "delete from t where pk = 3;"; referenced="1 2" ;;
    online_compaction)
        run_sql operation "delete from t where pk = 3;"
        referenced="1 2"
        before_restart=online_compaction
        ;;
    offline_compaction)
        run_sql operation "delete from t where pk = 3;"
        referenced="1 2"
        before_restart=offline_compaction
        ;;
    nondurable)
        # ID 1〜2 が durable な状態の epoch ファイルを退避してから、ID 3 を INSERT する
        shutdown_db
        cp "$case_dir/log/epoch" "$case_dir/epoch.saved"
        start_db
        run_sql operation "insert into t values (3, cast(X'03' as blob));"
        referenced="1 2"
        before_restart=rollback_epoch
        ;;
    esac

    if [[ $before_restart == online_compaction ]]; then
        # online_compaction_worker が ctrl/start_compaction を 1 秒間隔で拾う
        touch "$case_dir/log/ctrl/start_compaction"
        wait_for_log "compaction finished" 1
        wait_for_log "blob files garbage collection finished" 2  # 1 回目は起動時の GC、2 回目がオンラインの GC
    fi
    list_blobs >"$case_dir/blobs-2-before-restart.txt"
    shutdown_db
    if [[ $before_restart == offline_compaction ]]; then
        "$TGLOGUTIL" compaction --force "$case_dir/log" >"$case_dir/tglogutil.out" 2>&1 || fail "tglogutil compaction"
    elif [[ $before_restart == rollback_epoch ]]; then
        cp "$case_dir/epoch.saved" "$case_dir/log/epoch"
    fi

    start_db
    wait_for_log "blob files garbage collection finished" 1  # 起動時の BLOB GC (バックグラウンド) を待つ
    list_blobs >"$case_dir/blobs-3-after-restart.txt"
    run_sql insert "insert into $insert_table values (10, cast(X'BB' as blob));"
    list_blobs >"$case_dir/blobs-4-after-insert.txt"
    run_sql select "select pk from $insert_table order by pk;"
    shutdown_db

    # どの行からも参照されないのに、再起動後に残ったファイル
    local leftover=()
    local id
    while IFS=: read -r id _; do
        [[ " $referenced " == *" $id "* ]] || leftover+=("$id")
    done <"$case_dir/blobs-3-after-restart.txt"

    # 新しい BLOB の ID と、それが再起動後に残っていたファイルの上書きかどうか
    local new_ids new_id kind
    new_ids=$({ grep ':bb$' "$case_dir/blobs-4-after-insert.txt" || true; } | cut -d: -f1 | tr '\n' ' ')
    new_id=${new_ids% }
    if [[ -z $new_id || $new_id == *" "* ]]; then
        kind="(BB のファイルが 1 個ではない: '$new_id')"
    elif grep -q "^$new_id:" "$case_dir/blobs-3-after-restart.txt"; then
        kind="(上書き)"
    else
        kind="(新規)"
    fi

    local errors="なし"
    if [[ -n $(find "$case_dir/glog" -name 'tsurugidb.*.ERROR.*' -size +0 -print -quit) ]]; then
        errors="あり ($case_dir/glog)"
    fi
    local invalidated
    invalidated=$(cat "$case_dir"/glog/tsurugidb.*.INFO.* | grep -c 'marked invalid' || true)

    results+=("| $c | $(tr '\n' ' ' <"$case_dir/blobs-2-before-restart.txt" | sed 's/ $//') | ${leftover[*]:-なし} | $new_id $kind | $invalidated | $errors |")
}

for c in "${CASES[@]}"; do
    run_case "$c"
done

echo
echo "tsurugi_home: $TSURUGI_HOME"
grep -m1 -E '^- limestone ' "$TSURUGI_HOME/BUILDINFO.md" || true
echo
echo "| ケース | 再起動前の BLOB ファイル (ID:内容) | 不要なのに残ったファイル | 新しい BLOB の ID | 無効化したスニペット | ERROR ログ |"
echo "|---|---|---|---|---|---|"
printf '%s\n' "${results[@]}"
