# `tglogutil-repair` - repair a Tsurugi transaction log directory

`tglogutil repair` command repairs the contents of a corrupted Tsurugi transaction log directory.

## SYNOPSIS

```
$ tglogutil repair [options] <dblogdir>
```

## DESCRIPTION

Repair the transaction log data specified by `<dblogdir>`.
Specify the location set in the `log_location` parameter in the `[datastore]` section of the configuration file of Tsurugi server (`tsurugi.ini`).

Options:
* `--thread-num=<number>`
    * Number (default `1`) of concurrent processing thread of repair
* `--verbose=<bool>`
    * Verbose mode (default `false`)
* `--epoch=<epoch>`
    * Upper limit epoch number to be accepted as valid data (default is the value recorded in the transaction log directory)
* `--cut=<bool>`
    * Truncate the end of file when processing corruption-type (see section [TYPES OF CORRUPTION TO HANDLE](#types-of-corruption-to-handle) below) `truncated` and `damaged` (default `false`)
* `-h`, `--help`
    * Display usage information and exit

## EXIT STATUS

* 0: No errors
    * Repair process completed successfully
    * Nothing was done because the contents were already healthy before processing
* 16: Error
    * Errors left unrepaired
* 64 or more: Unable to handle
    * `dblogdir` does not exist
    * `dblogdir` is inaccessible
    * `dblogdir` has file format error
        * Specified a directory that is not the transaction log directory
        * Specified a transaction log directory of unsupported format version
        * `epoch` file does not exist

## TYPES OF CORRUPTION TO HANDLE

* Unpersisted transaction log entries exist
    * Finished writing the transaction log file to disk, but not finished writing the persistence marker (`nondurable`)
* The end of the transaction log file is corrupted
    * The transaction log file is only partially written to disk (`truncated`)
    * The end of the file is filled with zeros (`damaged`)

## PRECAUTIONS FOR USE

The repair process involves rewriting data, so it is recommended to back up the entire directory before using this tool.
