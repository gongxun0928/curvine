# AGENTS.md

## Cursor Cloud specific instructions

Curvine is a Rust distributed cache/file-system (master + worker + FUSE client + CLI),
plus a Vue web UI and optional Java/Python SDKs. Standard build/run commands live in
`README.md`, `Makefile`, and `build/build.sh` (`bash build/build.sh -h`). The notes below
only capture non-obvious, environment-specific gotchas for this cloud VM.

### Toolchain already provided by the VM snapshot
Rust 1.92 (pinned by `rust-toolchain.toml`, cargo at `/usr/local/cargo/bin`), gcc/g++ 13,
clang 18 + libclang (for bindgen), OpenJDK 21, Node 22 / npm 10, Python 3.12, plus the
extra system deps installed during setup: `protobuf-compiler` (protoc) and
`libfuse3-dev`/`fuse3`. Maven and `maturin` are NOT installed, so the Java SDK
(`-p java`) and Python SDK (`-p python`) are out of scope here; the runnable product is
`server`/`client`/`cli`/`fuse`/`web`.

### Building: you MUST set `CC=gcc CXX=g++`
The default `c++` on this VM is clang++, and clang cannot locate the libstdc++ headers
(a bare `#include <cstdint>` fails with `'cstdint' file not found`). This breaks the
C++ dependency `librocksdb-sys`. Always build with:

```bash
CC=gcc CXX=g++ bash build/build.sh -p core -p fuse -p web -d
```

- Use `bash` (not `sh`) — `build/build.sh` uses bash-only syntax (`&>`); under dash its
  cargo detection wrongly reports "cargo is not installed". `make` handles this on Ubuntu.
- `-d` = debug (faster); drop it for a release build. Output goes to `build/dist/`.

### Running a local single-node cluster
From `build/dist/`: `bin/local-cluster.sh start|status|stop` (starts master + worker),
then `bin/cv report` and `bin/cv fs mkdir|put|ls|cat ...`. Web UI: http://localhost:9000
(master also listens on 8995). `export CURVINE_CONF_FILE=$PWD/conf/curvine-cluster.toml`
so `bin/cv` finds the config.

- First start needs formatting: the master refuses to start with `format_master=false`
  and no existing RocksDB dirs. Set `format_master = true` and `format_worker = true` at
  the top of `conf/curvine-cluster.toml` for the very first start, then set them back to
  `false` (leaving them `true` wipes metadata/data on every restart).

### FUSE mount requires root
`curvine-fuse` mounts via a direct `mount()` syscall (needs CAP_SYS_ADMIN), so the
unprivileged `ubuntu` user gets `Operation not permitted`. `/dev/fuse` exists; run the
binary under `sudo`, e.g.:

```bash
sudo ./lib/curvine-fuse --mnt-path=$PWD/curvine-mnt --conf $PWD/conf/curvine-cluster.toml &
```

The default mount point `/curvine-fuse` also needs root/a writable path — pass
`--mnt-path` to a directory you can write.

### Tests
`cargo test -p <crate>` works (set `CC=gcc CXX=g++`). Full `build/run-tests.sh` starts a
`test_cluster` example first. Known environmental failure: `orpc`'s
`test_tmpfs_filesystem_detection_on_linux` asserts `/run` is tmpfs, but `/run` is
`overlayfs` in this container — it is not a code bug.
