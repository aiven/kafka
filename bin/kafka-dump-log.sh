#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

CURRENT_USER="$(id -un)"
CURRENT_GROUP="$(id -gn)"

if [ "$CURRENT_USER" != "kafka" ] || [ "$CURRENT_GROUP" != "kafka" ]; then
    echo "User is not 'kafka'! This script cannot be run as it may create empty index files which Kafka will not be able to read."
    exit 1
fi

exec $(dirname $0)/kafka-run-class.sh kafka.tools.DumpLogSegments "$@"
