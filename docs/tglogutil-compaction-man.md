# `tglogutil-compaction` - reorganize a Tsurugi transaction log directory

`tglogutil compaction` command reorganizes a Tsurugi transaction log directory that contains redundant data to reduce disk space usage.

## SYNOPSIS

```
$ tglogutil compaction [options] <dblogdir>
```

## DESCRIPTION

Reorganize the transaction log data specified by `<dblogdir>`.
Specify the location set in the `log_location` parameter in the `[datastore]` section of the configuration file of Tsurugi server (`tsurugi.ini`).

Options that are valid only for other subcommands are ignored if specified.

Options:
* `--force=<bool>`
    * If `true`, do not prompt before processing (default `false`)
* `--dry-run=<bool>`
    * If `true`, run in dry-run mode: perform the compaction against a temporary directory to verify that it would succeed, but leave `dblogdir` unchanged (default `false`)
    * Even in dry-run mode the temporary directory is populated with the compacted pwal, so it requires free space comparable to a real run except for blob data, which is not copied in dry-run mode; the temporary directory is removed on completion.
* `--thread-num=<number>`
    * Number (default `1`) of concurrent processing thread of reading log files
* `--working-dir=</path/to/working-dir>`
    * Directory for storing temporary files (default is a uniquely named directory next to `dblogdir`)
    * Must be on the same filesystem as `dblogdir`. If it is on a different filesystem, the command stops before any destructive operation and exits with status 64.
* `--verbose=<bool>`
    * Verbose mode (default `false`)
* `--make-backup=<bool>`
    * If `true`, the original contents of `dblogdir` are not removed but kept by renaming them to another directory on the same filesystem next to `dblogdir` (default `false`). This is not an independent backup: it resides on the same filesystem and is not a copy made elsewhere.
    * When `true`, blob data is copied (not moved) so that both the compacted directory and the renamed directory retain it. This requires additional free space roughly equal to the size of the blob data. If the filesystem does not have enough free space, the command may fail. In that case, back up the directory manually to another location and restore it as needed instead of using this option.
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
    * `working-dir` is on a different filesystem than `dblogdir`
    * files in `dblogdir` are damaged

## PRECAUTIONS FOR USE

The compaction process involves rewriting data, so it is recommended to back up the entire directory before using this tool.
