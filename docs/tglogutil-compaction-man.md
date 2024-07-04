# `tglogutil-compaction` - reorganize a Tsurugi transaction log directory

`tglogutil compaction` command reorganizes a Tsurugi transaction log directory that contains redundant data to reduce disk space usage.

## SYNOPSIS

```
$ tglogutil compaction [options] <dblogdir>
```

## DESCRIPTION

Reorganize the transaction log data specified by `<dblogdir>`.
Specify the location set in the `log_location` parameter in the `[datastore]` section of the configuration file of Tsurugi server (`tsurugi.ini`).

Options:
* `--force=<bool>`
    * If `true`, do not prompt before processing (default `false`)
* `--dry-run=<bool>`
    * Dry run mode. If `true`, transaction log files are not modified (default `false`)
* `--thread-num=<number>`
    * Number (default `1`) of concurrent processing thread of reading log files
* `--working-dir=</path/to/working-dir>`
    * Directory for storing temporary files (default is a uniquely named directory next to `dblogdir`)
* `--verbose=<bool>`
    * Verbose mode (default `false`)
* `--epoch=<epoch>`
    * Upper limit epoch number to be accepted as valid data (default is the value recorded in the transaction log directory)
* `--make-backup=<bool>`
    * Keep a backup of original data. If `false`, the contents of dblogdir will be removed (default `false`)
* `-h`, `--help`
    * Display usage information and exit

## EXIT STATUS

* 0: No errors
    * Compaction process completed successfully
* 16: Error
    * Failed to remove temporary directory
* 64 or more: Unable to handle
    * `dblogdir` does not exist
    * `dblogdir` is inaccessible
    * `dblogdir` has file format error
        * Specified a directory that is not the transaction log directory
        * Specified a transaction log directory of unsupported format version
        * `epoch` file does not exist
    * files in `dblogdir` are damaged

## PRECAUTIONS FOR USE

The compaction process involves rewriting data, so it is recommended to back up the entire directory before using this tool.
