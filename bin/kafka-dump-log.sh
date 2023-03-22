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

user=$(whoami)
user_error_explanation="
This command produces index files that must be readable by Kafka. To make
sure they are, the command should be executed as the same user that the
Kafka broker is running as."

if [[ "$user" == "root" ]]; then
  >&2 echo "error: Refusing to run command as root. $user_error_explanation"
  exit 1
elif [[ "$user" == "kafka" ]]; then
  >&2 echo "warning: You're running this command as '$user'. $user_error_explanation"
fi

exec $(dirname $0)/kafka-run-class.sh kafka.tools.DumpLogSegments "$@"
