# Limestone - a datastore engine

## Requirements

* CMake `>= 3.16`
* C++ Compiler `>= C++17`
* and see *Dockerfile* section

```sh
# retrieve third party modules
git submodule update --init --recursive
```

### Dockerfile

```dockerfile
FROM ubuntu:22.04

RUN apt update -y && apt install -y git build-essential cmake ninja-build libboost-filesystem-dev libboost-system-dev libboost-container-dev libboost-thread-dev libgoogle-glog-dev libgflags-dev doxygen libleveldb-dev librocksdb-dev pkg-config nlohmann-json3-dev
# libleveldb-dev is not required if -DRECOVERY_SORTER_KVSLIB=ROCKSDB
# librocksdb-dev is not required if -DRECOVERY_SORTER_KVSLIB=LEVELDB
```

optional packages:

* `clang-tidy`

## How to build

```sh
mkdir -p build
cd build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release ..
cmake --build .
```

available options:
* `-DCMAKE_INSTALL_PREFIX=<installation directory>` - change install location
* `-DCMAKE_PREFIX_PATH=<installation directory>` - indicate prerequisite installation directory
* `-DCMAKE_IGNORE_PATH="/usr/local/include;/usr/local/lib/"` - specify the libraries search paths to ignore. This is convenient if the environment has conflicting version installed on system default search paths. (e.g. gflags in /usr/local)
* `-DBUILD_SHARED_LIBS=OFF` - create static libraries instead of shared libraries
* `-DBUILD_TESTS=OFF` - don't build test programs
* `-DBUILD_DOCUMENTS=OFF` - don't build documents by doxygen
* `-DBUILD_STRICT=OFF` - don't treat compile warnings as build errors
* `-DINSTALL_EXAMPLES=ON` - install example applications
* `-DINSTALL_EXPERIMENTAL_TOOLS=ON` - install experimental tools (e.g. tgreplica)
* `-DRECOVERY_SORTER_KVSLIB=<library>` - select the eKVS library using at recovery process. (`LEVELDB` or `ROCKSDB` (default), case-insensitive)
* `-DRECOVERY_SORTER_PUT_ONLY=OFF` - don't use (faster) put-only method at recovery process
* `-DBUILD_REPLICATION_TESTS=ON` - (temporary) enable experimental replication tests (excluded by default)
* `-DENABLE_ALTIMETER=ON` - turn on the `altimeter logging`
* for debugging only
  * `-DENABLE_SANITIZER=OFF` - disable sanitizers (requires `-DCMAKE_BUILD_TYPE=Debug`)
  * `-DENABLE_UB_SANITIZER=ON` - enable undefined behavior sanitizer (requires `-DENABLE_SANITIZER=ON`)
  * `-DENABLE_COVERAGE=ON` - enable code coverage analysis (requires `-DCMAKE_BUILD_TYPE=Debug`)
  * `-DTRACY_ENABLE=ON` - enable tracy profiler for multi-thread debugging. See section below.
  * `-DLIKWID_ENABLE=ON` - enable LIKWID for performance metrics. See section below.

### install

```sh
cmake --build . --target install
```

### run tests

```sh
ctest -V
```

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
