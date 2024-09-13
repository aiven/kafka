/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.mirror.admin.offsetinspector;

public class TopicPartitionState {

    private final Long earliest;
    private final Long latest;

    public TopicPartitionState(Long earliest, Long latest) {
        this.earliest = earliest;
        this.latest = latest;
    }


    public boolean isEmpty() {
        return earliest == null || earliest.equals(latest);
    }

    public Long earliestOffset() {
        return earliest;
    }

    public Long latestOffset() {
        return latest;
    }

    public static final TopicPartitionState EMPTY = new TopicPartitionState(null, null);

}
