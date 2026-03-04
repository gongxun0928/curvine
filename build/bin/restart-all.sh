#!/bin/bash
#
# Copyright 2025 OPPO.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Get the absolute path to the directory where the script is located
BIN_DIR="$(cd "`dirname "$0"`"; pwd)"
CURVINE_HOME="$(cd "$BIN_DIR/.."; pwd)"

. "$CURVINE_HOME/conf/curvine-env.sh"

# Close all services and restart.

# Function to wait for a process to start
wait_for_process() {
    local service_name=$1
    local timeout=30
    local count=0
    
    echo "Waiting for $service_name to start..."
    while [ $count -lt $timeout ]; do
        if ps -ef | grep "curvine" | grep "$service_name" | grep -v grep > /dev/null; then
            echo "$service_name started successfully"
            return 0
        fi
        sleep 1
        count=$((count + 1))
    done
    
    echo "Warning: $service_name did not start within $timeout seconds"
    return 1
}

is_remote_scheduler_enabled() {
    if [ ! -f "$CURVINE_CONF_FILE" ]; then
        echo "false"
        return
    fi

    awk '
        BEGIN { in_job = 0; enabled = "false" }
        /^\[job\]/ { in_job = 1; next }
        /^\[/ { in_job = 0 }
        in_job && $1 ~ /^enable_remote_scheduler/ {
            gsub(/[[:space:]]/, "", $0);
            split($0, kv, "=");
            if (tolower(kv[2]) == "true") {
                enabled = "true";
            }
            print enabled;
            exit;
        }
        END {
            if (NR > 0 && enabled == "false") {
                print "false";
            }
        }
    ' "$CURVINE_CONF_FILE" | tail -n 1
}

umount -l /curvine-fuse
"${BIN_DIR}/curvine-fuse.sh" stop > /dev/null 2>&1
"${BIN_DIR}/local-cluster.sh" stop force

# Wait a moment for processes to be killed
sleep 3

# Start cluster services
"${BIN_DIR}/local-cluster.sh" start force

# Wait for services to start
if [ "$(is_remote_scheduler_enabled)" = "true" ]; then
wait_for_process "scheduler"
fi
wait_for_process "master"
wait_for_process "worker"

# Start fuse service
${BIN_DIR}/curvine-fuse.sh start
