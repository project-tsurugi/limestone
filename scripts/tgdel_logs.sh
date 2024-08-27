#!/bin/bash

# Usage function
usage() {
    echo "usage: tgdel_logs.sh <dblogdir>"
    exit 1
}

# Check arguments
if [ "$#" -ne 1 ]; then
    usage
fi

DBLOGDIR=$1
COMPACTION_CATALOG="$DBLOGDIR/compaction_catalog"
FILES_DELETED=0

# Validate dblogdir and compaction_catalog
if [ ! -d "$DBLOGDIR" ] || [ ! -d "$DBLOGDIR/ctrl" ] || [ ! -f "$COMPACTION_CATALOG" ]; then
    echo "Error: '$DBLOGDIR' is not a valid dblogdir."
    usage
fi

# Delete detached pwal files
while read -r line; do
    if [[ $line == DETACHED_PWAL* ]]; then
        FILENAME=$(echo "$line" | /usr/bin/awk '{print $2}')
        FILE_TO_DELETE="$DBLOGDIR/$FILENAME"
        if [ -f "$FILE_TO_DELETE" ]; then
            echo "Deleting: $FILE_TO_DELETE"
            /usr/bin/rm "$FILE_TO_DELETE"
            FILES_DELETED=$((FILES_DELETED + 1))
        fi
    fi
done < "$COMPACTION_CATALOG"

# Check if no files were deleted
if [ "$FILES_DELETED" -eq 0 ]; then
    echo "No files could be deleted."
fi

exit 0

