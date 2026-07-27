#!/usr/bin/env python3
"""Analyze EPOCHTRACE lines emitted by the epoch_trace instrumentation.

Replays the linearized event log (switch_begin / switch_end / session_begin /
session_end) and reports, for every switch_epoch completion, whether log
channels were still active with a session epoch smaller than the epoch just
switched to (i.e. sessions spanning the epoch switch), plus how far the
informed (durable) epoch lags behind.

Usage:
    analyze_epoch_trace.py <logfile>...

Reads glog output; any line without an EPOCHTRACE marker is ignored.
"""

import re
import sys
from collections import Counter

LINE_RE = re.compile(
    r"EPOCHTRACE (?P<event>\w+) ch=(?P<ch>\S+) epoch=(?P<epoch>\d+)"
    r" switched=(?P<switched>\d+) informed=(?P<informed>\d+)"
)


def main(paths):
    active = {}  # channel -> session epoch
    switch_count = 0
    switches_with_spanning = 0
    spanning_count_hist = Counter()  # number of spanning sessions -> occurrences
    max_precede = 0  # max (switch epoch - active session epoch)
    max_informed_lag = 0  # max ((switch epoch - 1) - informed)
    session_span_hist = Counter()  # (switched at end - session epoch) -> occurrences
    session_count = 0
    examples = []
    # group-commit view: at each gc_record_end / gc_notify_end for epoch X,
    # active sessions (all have epoch > X) are the ones running ahead of the
    # group commit that just completed
    gc_stats = {
        "gc_record_end": {"count": 0, "with_active": 0, "max_precede": 0,
                          "active_hist": Counter(), "examples": []},
        "gc_notify_end": {"count": 0, "with_active": 0, "max_precede": 0,
                          "active_hist": Counter(), "examples": []},
    }

    for path in paths:
        with open(path, "r", errors="replace") as f:
            for line in f:
                m = LINE_RE.search(line)
                if not m:
                    continue
                event = m.group("event")
                ch = m.group("ch")
                epoch = int(m.group("epoch"))
                switched = int(m.group("switched"))
                informed = int(m.group("informed"))

                if event == "session_begin":
                    active[ch] = epoch
                elif event == "session_end":
                    active.pop(ch, None)
                    session_count += 1
                    session_span_hist[switched - epoch] += 1
                elif event == "switch_end":
                    switch_count += 1
                    spanning = {c: e for c, e in active.items() if e < epoch}
                    spanning_count_hist[len(spanning)] += 1
                    if spanning:
                        switches_with_spanning += 1
                        precede = epoch - min(spanning.values())
                        max_precede = max(max_precede, precede)
                        if len(examples) < 10:
                            examples.append((epoch, informed, dict(spanning)))
                    lag = (epoch - 1) - informed
                    max_informed_lag = max(max_informed_lag, lag)
                elif event in ("gc_record_end", "gc_notify_end"):
                    st = gc_stats[event]
                    st["count"] += 1
                    ahead = {c: e for c, e in active.items() if e > epoch}
                    st["active_hist"][len(ahead)] += 1
                    if ahead:
                        st["with_active"] += 1
                        precede = max(ahead.values()) - epoch
                        st["max_precede"] = max(st["max_precede"], precede)
                        if len(st["examples"]) < 5:
                            st["examples"].append((epoch, dict(ahead)))
                # switch_begin / gc_record_begin / gc_notify_begin are not
                # needed for the reconstruction

    print(f"switch_epoch completions          : {switch_count}")
    if switch_count > 0:
        pct = 100.0 * switches_with_spanning / switch_count
        print(f"  with spanning active sessions   : {switches_with_spanning} ({pct:.2f}%)")
    print(f"  max precede (switch - session)  : {max_precede}")
    print(f"  max informed lag ((N-1) - inf)  : {max_informed_lag}")
    print(f"sessions ended                    : {session_count}")
    print("session span histogram (switched_at_end - session_epoch : count):")
    for span in sorted(session_span_hist):
        print(f"  {span:6d} : {session_span_hist[span]}")
    print("spanning sessions per switch histogram (count : occurrences):")
    for n in sorted(spanning_count_hist):
        print(f"  {n:6d} : {spanning_count_hist[n]}")
    if examples:
        print("first spanning examples (switch epoch, informed, {channel: session epoch}):")
        for epoch, informed, spanning in examples:
            print(f"  switch={epoch} informed={informed} spanning={spanning}")

    for event, label in (("gc_record_end", "group commit record (epoch file written)"),
                         ("gc_notify_end", "group commit notify (persistent callback)")):
        st = gc_stats[event]
        print(f"{label}:")
        print(f"  completions                     : {st['count']}")
        if st["count"] > 0:
            pct = 100.0 * st["with_active"] / st["count"]
            print(f"  with sessions running ahead     : {st['with_active']} ({pct:.2f}%)")
            print(f"  max precede (session - epoch)   : {st['max_precede']}")
            print("  sessions-ahead histogram (count : occurrences):")
            for n in sorted(st["active_hist"]):
                print(f"    {n:6d} : {st['active_hist'][n]}")
            for epoch, ahead in st["examples"]:
                print(f"  example: committed={epoch} ahead={ahead}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)
    main(sys.argv[1:])
