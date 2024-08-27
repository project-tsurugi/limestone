#!/bin/bash

# Usage function
usage() {
    echo "usage: tgcmpct_logs.sh <dblogdir>"
    exit 1
}

# Check arguments
if [ "$#" -ne 1 ]; then
    usage
fi

DBLOGDIR=$1

# Validate dblogdir
if [ ! -d "$DBLOGDIR" ] || [ ! -f "$DBLOGDIR/compaction_catalog" ] || [ ! -d "$DBLOGDIR/ctrl" ]; then
    echo "Error: '$DBLOGDIR' is not a valid dblogdir."
    usage
fi

# Request to start compaction
if /usr/bin/touch "$DBLOGDIR/ctrl/start_compaction"; then
    echo "Compaction request has been made."
else
    echo "Error: Failed to request compaction."
    exit 1
fi

exit 0

