# Curvine Development Guide

Curvine is a high-performance distributed cache system written in Rust. See `README.md` for full overview.

## Cursor Cloud specific instructions

### Build environment

- **`CXX=g++` is required** when building. The default `c++` symlink points to `clang++`, which cannot locate the GCC C++ standard library headers needed by the RocksDB build script. Always `export CXX=g++` before running cargo commands.
- A `libstdc++.so` symlink must exist at `/usr/lib/x86_64-linux-gnu/libstdc++.so` (pointing to the versioned `.so.6`). If linking fails with `unable to find library -lstdc++`, recreate it: `sudo ln -sf /usr/lib/gcc/x86_64-linux-gnu/13/libstdc++.so /usr/lib/x86_64-linux-gnu/libstdc++.so`.

### Key commands

| Task | Command |
|---|---|
| Build core (release) | `CXX=g++ bash build/build.sh -p core --skip-java-sdk` |
| Build core (debug) | `CXX=g++ bash build/build.sh -p core --skip-java-sdk -d` |
| Build all | `CXX=g++ make all` |
| Format check | `cargo fmt -- --check` |
| Clippy lint | `CXX=g++ cargo clippy --all-targets -- --deny warnings --allow clippy::uninlined-format-args` |
| Unit tests | `CXX=g++ cargo test -p curvine-common -p curvine-server --lib -p orpc` |
| Full test suite | `CXX=g++ bash build/run-tests.sh` (starts a test cluster, runs all tests) |
| Web UI build | `cd curvine-web/webui && npm install && npm run build` |
| Web UI lint | `cd curvine-web/webui && npx vue-cli-service lint --no-fix` |

### Starting a single-node cluster

After building core in release mode, the binaries go to `build/dist/`. To start:

```bash
cd build/dist
export CURVINE_MASTER_HOSTNAME=localhost
export CURVINE_WORKER_HOSTNAME=localhost
bin/curvine-master.sh start
bin/curvine-worker.sh start
```

The Web UI (Vue 3) is served by the master on port **9000** but requires static assets at `build/dist/webui/`. After building the web UI, copy them: `cp -r curvine-web/webui/dist build/dist/webui`.

CLI tool at `build/dist/bin/cv`:
- `bin/cv report` — cluster overview
- `bin/cv fs ls /` — list filesystem root
- `bin/cv fs mkdir /dir` — create directory
- `bin/cv fs put <local> <remote>` — upload file

### Known environment-specific test failures

- `orpc::libc_test::test_tmpfs_filesystem_detection_on_linux` fails in container environments because `/run` is not a tmpfs. This is not a code bug.
