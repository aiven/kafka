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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TopicPartitionStateCollector {

    private final Duration adminTimeout;
    private final AdminClient adminClient;
    private final Set<TopicPartition> topicPartitions;

    private TopicPartitionStateCollector(final Duration adminTimeout,
                                         final AdminClient adminClient,
                                         final Set<TopicPartition> topicPartitions) {

        this.adminTimeout = adminTimeout;
        this.adminClient = adminClient;
        this.topicPartitions = topicPartitions;
    }

    public static TopicPartitionStateCollector.TopicPartitionStateCollectorBuilder builder() {
        return new TopicPartitionStateCollector.TopicPartitionStateCollectorBuilder();
    }

    public Map<TopicPartition, TopicPartitionState> getTopicStates() {

        Map<TopicPartition, OffsetSpec> earliestPartitionOffsets = topicPartitions.stream()
                .collect(Collectors.toMap(Function.identity(), ignored -> OffsetSpec.earliest()));
        Map<TopicPartition, OffsetSpec> latestPartitionOffsets = topicPartitions.stream()
                .collect(Collectors.toMap(Function.identity(), ignored -> OffsetSpec.latest()));

        final Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> listOffsetsResultInfoLatest;
        final Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> listOffsetsResultInfoEarliest;
        try {
            listOffsetsResultInfoEarliest = adminClient.listOffsets(earliestPartitionOffsets)
                    .all()
                    .get(adminTimeout.toMillis(), TimeUnit.MILLISECONDS);
            listOffsetsResultInfoLatest = adminClient.listOffsets(latestPartitionOffsets)
                    .all()
                    .get(adminTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
        return topicPartitions.stream()
                .collect(Collectors.toMap(Function.identity(), tp -> new TopicPartitionState(listOffsetsResultInfoEarliest.get(tp).offset(), listOffsetsResultInfoLatest.get(tp).offset())));
    }

    public static final class TopicPartitionStateCollectorBuilder {
        private Duration adminTimeout = Duration.ofMinutes(1);
        private AdminClient adminClient;
        private Set<TopicPartition> topicPartitions;

        private TopicPartitionStateCollectorBuilder() {
            /* hide constructor */
        }

        public TopicPartitionStateCollector.TopicPartitionStateCollectorBuilder withAdminTimeout(final Duration adminTimeout) {
            this.adminTimeout = adminTimeout;
            return this;
        }

        public TopicPartitionStateCollector.TopicPartitionStateCollectorBuilder withAdminClient(final AdminClient adminClient) {
            this.adminClient = adminClient;
            return this;
        }

        public TopicPartitionStateCollector.TopicPartitionStateCollectorBuilder withTopicPartitions(
                final Set<TopicPartition> topicPartitions) {
            this.topicPartitions = topicPartitions;
            return this;
        }

        public TopicPartitionStateCollector build() {
            return new TopicPartitionStateCollector(adminTimeout, adminClient, topicPartitions);
        }

    }
}
