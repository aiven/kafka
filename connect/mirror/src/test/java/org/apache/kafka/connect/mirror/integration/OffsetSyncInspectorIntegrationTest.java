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
package org.apache.kafka.connect.mirror.integration;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.mirror.IdentityReplicationPolicy;
import org.apache.kafka.connect.mirror.MirrorHeartbeatConnector;
import org.apache.kafka.connect.mirror.MirrorMakerConfig;
import org.apache.kafka.connect.mirror.SourceAndTarget;
import org.apache.kafka.connect.mirror.admin.ConsumerGroupOffsetSyncInspector;
import org.apache.kafka.connect.mirror.admin.offsetinspector.ConsumerGroupOffsetsComparer;
import org.apache.kafka.connect.mirror.admin.offsetinspector.GroupAndState;

import org.apache.kafka.connect.mirror.admin.offsetinspector.TopicPartitionState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.kafka.connect.mirror.admin.offsetinspector.ConsumerGroupOffsetsComparer.NOT_SYNCED_EMPTY_PARTITION_OTHER_SUCCESSFUL_MESSAGE;
import static org.apache.kafka.connect.mirror.admin.offsetinspector.ConsumerGroupOffsetsComparer.SUCCESS_MESSAGE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests MM2 offset sync inspector tool.
 *
 * <p>MM2 is configured with active/passive replication between two Kafka clusters with {@link IdentityReplicationPolicy}.
 */
@Tag("integration")
public class OffsetSyncInspectorIntegrationTest extends MirrorConnectorsIntegrationSetupBase {
    @BeforeEach
    public void startClusters() throws Exception {
        super.startClusters(new HashMap<String, String>() {{
                put("replication.policy.class", IdentityReplicationPolicy.class.getName());
                // one way replication from primary to backup, add topic filter
                put(BACKUP_CLUSTER_ALIAS + "->" + PRIMARY_CLUSTER_ALIAS + ".enabled", "false");
                put(PRIMARY_CLUSTER_ALIAS + "->" + BACKUP_CLUSTER_ALIAS + ".enabled", "true");
                // enable automated consumer group offset sync
                put("sync.group.offsets.enabled", "true");
                put("sync.group.offsets.interval.seconds", "1");
            }});
    }

    @Test
    public void testOffsetSyncInspectionForFullySyncedGroup() throws Exception {
        // test-topic-1 is created by the base test class.
        final String testTopic1Name = "test-topic-1";
        final String consumerGroupTopic1 = "consumer-group-topic-1";
        final Map<String, Object> consumerGroupTopic1Props = new HashMap<String, Object>() {{
                put("group.id", consumerGroupTopic1);
                put("auto.offset.reset", "latest");
            }};

        try (final Consumer<byte[], byte[]> testTopic1Consumer = primary.kafka().createConsumerAndSubscribeTo(consumerGroupTopic1Props, testTopic1Name)) {
            testTopic1Consumer.poll(CONSUMER_POLL_TIMEOUT);
            testTopic1Consumer.commitSync();

            mm2Props.put("offset.lag.max", "0");
            mm2Props.put(PRIMARY_CLUSTER_ALIAS + "->" + BACKUP_CLUSTER_ALIAS + ".topics", testTopic1Name);
            mm2Config = new MirrorMakerConfig(mm2Props);

            waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);
            waitUntilMirrorMakerIsRunning(primary, Collections.singletonList(MirrorHeartbeatConnector.class),
                    mm2Config, BACKUP_CLUSTER_ALIAS, PRIMARY_CLUSTER_ALIAS);

            // make sure the topic is auto-created in the other cluster
            waitForTopicCreated(primary, testTopic1Name);
            waitForTopicCreated(backup, testTopic1Name);

            // Fill all partitions in the test-topic-1
            produceMessages(primaryProducer, testTopic1Name, NUM_PARTITIONS);

            assertEquals(NUM_RECORDS_PRODUCED, primary.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, testTopic1Name).count(),
                    "Records were not produced to primary cluster.");
            assertEquals(NUM_RECORDS_PRODUCED, backup.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, testTopic1Name).count(),
                    "Records were not replicated to backup cluster.");

            testTopic1Consumer.poll(CONSUMER_POLL_TIMEOUT);
            testTopic1Consumer.commitSync();

            // sleep a bit to have MM2 finish offset syncing
            Thread.sleep(TimeUnit.SECONDS.toMillis(2));
            final ConsumerGroupOffsetSyncInspector consumerGroupOffsetSyncInspector = new ConsumerGroupOffsetSyncInspector();
            final Map<SourceAndTarget, ConsumerGroupOffsetsComparer.ConsumerGroupsCompareResult> clusterResults =
                    consumerGroupOffsetSyncInspector.inspect(mm2Props, Duration.ofMinutes(1), Duration.ofSeconds(30),
                            false, true);
            final ConsumerGroupOffsetsComparer.ConsumerGroupsCompareResult primaryToBackupResult =
                    clusterResults.get(new SourceAndTarget(PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS));

            // The dummy group should not be in the result as only active groups are inspected.
            final Set<ConsumerGroupOffsetsComparer.ConsumerGroupCompareResult> expectedConsumerGroupCompareResult
                    = primaryToBackupResult.getConsumerGroupsCompareResult();
            assertEquals(NUM_PARTITIONS, primaryToBackupResult.getConsumerGroupsCompareResult().size());
            for (int partition = 0; partition < NUM_PARTITIONS; partition++) {
                final ConsumerGroupOffsetsComparer.ConsumerGroupCompareResult result =
                        new ConsumerGroupOffsetsComparer.ConsumerGroupCompareResult(
                            new GroupAndState(consumerGroupTopic1, ConsumerGroupState.STABLE),
                                new TopicPartition(testTopic1Name, partition),
                                new TopicPartitionState(0L, (long) NUM_RECORDS_PER_PARTITION),
                                new TopicPartitionState(0L, (long) NUM_RECORDS_PER_PARTITION),
                                ((Integer) NUM_RECORDS_PER_PARTITION).longValue(),
                                121212L,
                                ((Integer) NUM_RECORDS_PER_PARTITION).longValue(),
                            9L, true, null, SUCCESS_MESSAGE);
                expectedConsumerGroupCompareResult.add(result);
            }
            assertEquals(expectedConsumerGroupCompareResult, primaryToBackupResult.getConsumerGroupsCompareResult());
            final ConsumerGroupOffsetsComparer.ConsumerGroupsCompareResult backupToPrimaryResult =
                    clusterResults.get(new SourceAndTarget(BACKUP_CLUSTER_ALIAS, PRIMARY_CLUSTER_ALIAS));
            assertNull(backupToPrimaryResult);
        }
    }

    @Test
    public void testOffsetSyncInspectionForEmptyGroup() throws Exception {
        // test-topic-1 is created by the base test class.
        final String testTopic1Name = "test-topic-1";
        final String consumerGroupTopic1 = "consumer-group-topic-1";
        final Map<String, Object> consumerGroupTopic1Props = new HashMap<String, Object>() {{
                put("group.id", consumerGroupTopic1);
                put("auto.offset.reset", "latest");
                put("enable.auto.commit", "false");
            }};

        Consumer<byte[], byte[]> testTopic1Consumer = null;
        try {
            testTopic1Consumer = primary.kafka().createConsumerAndSubscribeTo(consumerGroupTopic1Props, testTopic1Name);
            testTopic1Consumer.poll(CONSUMER_POLL_TIMEOUT);
            testTopic1Consumer.commitSync();

            mm2Props.put("offset.lag.max", "0");
            mm2Props.put(PRIMARY_CLUSTER_ALIAS + "->" + BACKUP_CLUSTER_ALIAS + ".topics", testTopic1Name);
            mm2Config = new MirrorMakerConfig(mm2Props);

            waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);
            waitUntilMirrorMakerIsRunning(primary, Collections.singletonList(MirrorHeartbeatConnector.class),
                    mm2Config, BACKUP_CLUSTER_ALIAS, PRIMARY_CLUSTER_ALIAS);

            // make sure the topic is auto-created in the other cluster
            waitForTopicCreated(primary, testTopic1Name);
            waitForTopicCreated(backup, testTopic1Name);

            // Fill all partitions in the test-topic-1
            produceMessages(primaryProducer, testTopic1Name, NUM_PARTITIONS);

            assertEquals(NUM_RECORDS_PRODUCED, primary.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, testTopic1Name).count(),
                    "Records were not produced to primary cluster.");
            assertEquals(NUM_RECORDS_PRODUCED, backup.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, testTopic1Name).count(),
                    "Records were not replicated to backup cluster.");

            final ConsumerRecords<byte[], byte[]> records = testTopic1Consumer.poll(CONSUMER_POLL_TIMEOUT);
            final Map<TopicPartition, OffsetAndMetadata> commitData = new HashMap<>();
            for (final ConsumerRecord<byte[], byte[]> record : records.records(testTopic1Name)) {
                if (record.partition() % 2 == 0) {
                    commitData.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset()));
                }
            }
            testTopic1Consumer.commitSync(commitData);

            // Close the consumer, this makes the consumer group as empty in Kafka.
            testTopic1Consumer.close();
            testTopic1Consumer = null;

            // sleep a bit to have MM2 finish offset syncing
            Thread.sleep(TimeUnit.SECONDS.toMillis(5));

            final ConsumerGroupOffsetSyncInspector consumerGroupOffsetSyncInspector = new ConsumerGroupOffsetSyncInspector();
            final Map<SourceAndTarget, ConsumerGroupOffsetsComparer.ConsumerGroupsCompareResult> clusterResults =
                    consumerGroupOffsetSyncInspector.inspect(mm2Props, Duration.ofMinutes(1), Duration.ofSeconds(30),
                            true, true);
            final ConsumerGroupOffsetsComparer.ConsumerGroupsCompareResult primaryToBackupResult =
                    clusterResults.get(new SourceAndTarget(PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS));

            final Set<ConsumerGroupOffsetsComparer.ConsumerGroupCompareResult> expectedConsumerGroupCompareResult =
                    new HashSet<>();
            for (int partition = 0; partition < NUM_PARTITIONS; partition++) {
                final ConsumerGroupOffsetsComparer.ConsumerGroupCompareResult result =
                        new ConsumerGroupOffsetsComparer.ConsumerGroupCompareResult(
                                new GroupAndState("consumer-group-dummy", ConsumerGroupState.EMPTY),
                                new TopicPartition(testTopic1Name, partition),
                                new TopicPartitionState(0L, (long) NUM_RECORDS_PER_PARTITION),
                                new TopicPartitionState(0L, (long) NUM_RECORDS_PER_PARTITION),
                                0L, 10L,
                                0L, 10L, true, null, SUCCESS_MESSAGE);
                expectedConsumerGroupCompareResult.add(result);
            }
            for (int partition = 0; partition < NUM_PARTITIONS; partition++) {
                final ConsumerGroupOffsetsComparer.ConsumerGroupCompareResult result;
                if (partition % 2 == 0) {
                    result = new ConsumerGroupOffsetsComparer.ConsumerGroupCompareResult(
                            new GroupAndState(consumerGroupTopic1, ConsumerGroupState.EMPTY),
                            new TopicPartition(testTopic1Name, partition),
                            new TopicPartitionState(0L, (long) NUM_RECORDS_PER_PARTITION),
                            new TopicPartitionState(0L, (long) NUM_RECORDS_PER_PARTITION),
                            9L, 1L,
                            9L, 1L, true, null, SUCCESS_MESSAGE);
                } else {
                    result = new ConsumerGroupOffsetsComparer.ConsumerGroupCompareResult(
                            new GroupAndState(consumerGroupTopic1, ConsumerGroupState.EMPTY),
                            new TopicPartition(testTopic1Name, partition),
                            new TopicPartitionState(0L, (long) NUM_RECORDS_PER_PARTITION),
                            new TopicPartitionState(0L, (long) NUM_RECORDS_PER_PARTITION),
                            0L, 10L,
                            0L, 10L, true, null, SUCCESS_MESSAGE);

                }
                expectedConsumerGroupCompareResult.add(result);
            }
            assertEquals(
                    expectedConsumerGroupCompareResult.stream().sorted().collect(Collectors.toList()),
                    primaryToBackupResult.getConsumerGroupsCompareResult().stream().sorted().collect(Collectors.toList()));

            final ConsumerGroupOffsetsComparer.ConsumerGroupsCompareResult backupToPrimaryResult =
                    clusterResults.get(new SourceAndTarget(BACKUP_CLUSTER_ALIAS, PRIMARY_CLUSTER_ALIAS));
            assertNull(backupToPrimaryResult);
        } finally {
            if (testTopic1Consumer != null) {
                testTopic1Consumer.close();
            }
        }
    }


    @Test
    public void testOffsetSyncInspectionForTopicHavingAnEmptyPartition() throws Exception {
        final String testTopic2Name = "test-topic-2-partition-0-filled";
        primary.kafka().createTopic(testTopic2Name, 2, 1, Collections.emptyMap(),
                Collections.singletonMap(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_DURATION_MS));

        final String consumerGroupTopicPartition0Filled = "consumer-group-test-topic-2-partition-0-filled";
        final Map<String, Object> consumerGroupTopic2Props = new HashMap<String, Object>() {{
                put("group.id", consumerGroupTopicPartition0Filled);
                put("auto.offset.reset", "latest");
            }};

        try (final Consumer<byte[], byte[]> testTopic2Consumer = primary.kafka().createConsumerAndSubscribeTo(consumerGroupTopic2Props, testTopic2Name)) {
            testTopic2Consumer.poll(CONSUMER_POLL_TIMEOUT);
            testTopic2Consumer.commitSync();

            mm2Props.put("offset.lag.max", "0");
            mm2Props.put(PRIMARY_CLUSTER_ALIAS + "->" + BACKUP_CLUSTER_ALIAS + ".topics", testTopic2Name);
            mm2Config = new MirrorMakerConfig(mm2Props);

            waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);
            waitUntilMirrorMakerIsRunning(primary, Collections.singletonList(MirrorHeartbeatConnector.class),
                    mm2Config, BACKUP_CLUSTER_ALIAS, PRIMARY_CLUSTER_ALIAS);

            // make sure the topic is auto-created in the other cluster
            waitForTopicCreated(primary, testTopic2Name);
            waitForTopicCreated(backup, testTopic2Name);

            // Fill first partition of the test topic.
            produceMessages(primaryProducer, testTopic2Name, 1);

            assertEquals(NUM_RECORDS_PER_PARTITION, primary.kafka().consume(NUM_RECORDS_PER_PARTITION, RECORD_TRANSFER_DURATION_MS, testTopic2Name).count(),
                    "Records were not produced to primary cluster.");
            assertEquals(NUM_RECORDS_PER_PARTITION, backup.kafka().consume(NUM_RECORDS_PER_PARTITION, RECORD_TRANSFER_DURATION_MS, testTopic2Name).count(),
                    "Records were not replicated to backup cluster.");

            // Move the consumer group to end of the topic.
            testTopic2Consumer.poll(CONSUMER_POLL_TIMEOUT);
            testTopic2Consumer.commitSync();

            // sleep a bit to have MM2 finish offset syncing
            Thread.sleep(TimeUnit.SECONDS.toMillis(2));
            final ConsumerGroupOffsetSyncInspector consumerGroupOffsetSyncInspector = new ConsumerGroupOffsetSyncInspector();
            final Map<SourceAndTarget, ConsumerGroupOffsetsComparer.ConsumerGroupsCompareResult> clusterResults =
                    consumerGroupOffsetSyncInspector.inspect(mm2Props, Duration.ofMinutes(1), Duration.ofSeconds(30),
                            false, true);
            final ConsumerGroupOffsetsComparer.ConsumerGroupsCompareResult primaryToBackupResult =
                    clusterResults.get(new SourceAndTarget(PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS));

            final Set<ConsumerGroupOffsetsComparer.ConsumerGroupCompareResult> expectedConsumerGroupCompareResult =
                    new HashSet<>();
            final Set<ConsumerGroupOffsetsComparer.ConsumerGroupCompareResult> consumerGroupCompareResult
                    = primaryToBackupResult.getConsumerGroupsCompareResult();
            assertEquals(2, consumerGroupCompareResult.size());
            for (int partition = 0; partition < 2; partition++) {
                final Long expectedSourceOffset = partition == 0 ? (long) NUM_RECORDS_PER_PARTITION : 0L;
                final Long expectedTargetOffset = partition == 0 ? (long) NUM_RECORDS_PER_PARTITION : null;
                final Long expectedTargetLag, expectedLagAtTargetToSource;
                expectedTargetLag = expectedLagAtTargetToSource = partition == 0 ? 0L : null;
                final String message = partition == 0 ? SUCCESS_MESSAGE : NOT_SYNCED_EMPTY_PARTITION_OTHER_SUCCESSFUL_MESSAGE;
                final String blockingComponent = partition == 0 ? null : "SOURCE PARTITION";
                final ConsumerGroupOffsetsComparer.ConsumerGroupCompareResult result =
                        new ConsumerGroupOffsetsComparer.ConsumerGroupCompareResult(
                                new GroupAndState(consumerGroupTopicPartition0Filled, ConsumerGroupState.STABLE),
                                new TopicPartition(testTopic2Name, partition),
                                new TopicPartitionState(0L, (long) NUM_RECORDS_PER_PARTITION),
                                new TopicPartitionState(0L, (long) NUM_RECORDS_PER_PARTITION),
                                expectedSourceOffset, expectedLagAtTargetToSource,
                                expectedTargetOffset, expectedTargetLag,
                                true, blockingComponent, message);
                expectedConsumerGroupCompareResult.add(result);
            }

            assertEquals(
                    expectedConsumerGroupCompareResult.stream().sorted().collect(Collectors.toList()),
                    primaryToBackupResult.getConsumerGroupsCompareResult().stream().sorted().collect(Collectors.toList()));

            final ConsumerGroupOffsetsComparer.ConsumerGroupsCompareResult backupToPrimaryResult =
                    clusterResults.get(new SourceAndTarget(BACKUP_CLUSTER_ALIAS, PRIMARY_CLUSTER_ALIAS));
            assertNull(backupToPrimaryResult);
        }
    }

    @Test
    public void testOffsetSyncInspectionForTopicHavingAnEmptyPartitionFilterOkGroupsOut() throws Exception {
        final String testTopic2Name = "test-topic-2-partition-0-filled";
        primary.kafka().createTopic(testTopic2Name, 2, 1, Collections.emptyMap(),
                Collections.singletonMap(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_DURATION_MS));

        final String consumerGroupTopicPartition0Filled = "consumer-group-test-topic-2-partition-0-filled";
        final Map<String, Object> consumerGroupTopic2Props = new HashMap<String, Object>() {{
                put("group.id", consumerGroupTopicPartition0Filled);
                put("auto.offset.reset", "latest");
            }};

        try (final Consumer<byte[], byte[]> testTopic2Consumer = primary.kafka().createConsumerAndSubscribeTo(consumerGroupTopic2Props, testTopic2Name)) {
            testTopic2Consumer.poll(CONSUMER_POLL_TIMEOUT);
            testTopic2Consumer.commitSync();

            mm2Props.put("offset.lag.max", "0");
            mm2Props.put(PRIMARY_CLUSTER_ALIAS + "->" + BACKUP_CLUSTER_ALIAS + ".topics", testTopic2Name);
            mm2Config = new MirrorMakerConfig(mm2Props);

            waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);
            waitUntilMirrorMakerIsRunning(primary, Collections.singletonList(MirrorHeartbeatConnector.class),
                    mm2Config, BACKUP_CLUSTER_ALIAS, PRIMARY_CLUSTER_ALIAS);

            // make sure the topic is auto-created in the other cluster
            waitForTopicCreated(primary, testTopic2Name);
            waitForTopicCreated(backup, testTopic2Name);

            // Fill first partition of the test topic.
            produceMessages(primaryProducer, testTopic2Name, 1);

            assertEquals(NUM_RECORDS_PER_PARTITION, primary.kafka().consume(NUM_RECORDS_PER_PARTITION, RECORD_TRANSFER_DURATION_MS, testTopic2Name).count(),
                    "Records were not produced to primary cluster.");
            assertEquals(NUM_RECORDS_PER_PARTITION, backup.kafka().consume(NUM_RECORDS_PER_PARTITION, RECORD_TRANSFER_DURATION_MS, testTopic2Name).count(),
                    "Records were not replicated to backup cluster.");

            // Move the consumer group to end of the topic.
            testTopic2Consumer.poll(CONSUMER_POLL_TIMEOUT);
            testTopic2Consumer.commitSync();

            // sleep a bit to have MM2 finish offset syncing
            Thread.sleep(TimeUnit.SECONDS.toMillis(2));
            final ConsumerGroupOffsetSyncInspector consumerGroupOffsetSyncInspector = new ConsumerGroupOffsetSyncInspector();
            final Map<SourceAndTarget, ConsumerGroupOffsetsComparer.ConsumerGroupsCompareResult> clusterResults =
                    consumerGroupOffsetSyncInspector.inspect(mm2Props, Duration.ofMinutes(1), Duration.ofSeconds(30),
                            false, false);
            final ConsumerGroupOffsetsComparer.ConsumerGroupsCompareResult primaryToBackupResult =
                    clusterResults.get(new SourceAndTarget(PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS));

            final Set<ConsumerGroupOffsetsComparer.ConsumerGroupCompareResult> consumerGroupCompareResult
                    = primaryToBackupResult.getConsumerGroupsCompareResult();
            assertEquals(0, consumerGroupCompareResult.size());

            final ConsumerGroupOffsetsComparer.ConsumerGroupsCompareResult backupToPrimaryResult =
                    clusterResults.get(new SourceAndTarget(BACKUP_CLUSTER_ALIAS, PRIMARY_CLUSTER_ALIAS));
            assertNull(backupToPrimaryResult);
        }
    }

    @Test
    public void testOffsetSyncInspectionNoResultForExcludedTopic() throws Exception {
        final String testTopic1Name = "test-topic-1";
        final String notMirroredTestTopicName = "not-mirrored-test-topic";
        primary.kafka().createTopic(notMirroredTestTopicName, 2, 1, Collections.emptyMap(),
                Collections.singletonMap(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_DURATION_MS));

        // warm up consumers before starting the connectors so we don't need to wait for discovery
        final String consumerGroupNotMirroredTopic = "consumer-group-not-mirrored-test-topic";
        final Map<String, Object> consumerGroupNotMirroredTopicProps = new HashMap<String, Object>() {{
                put("group.id", consumerGroupNotMirroredTopic);
                put("auto.offset.reset", "latest");
            }};

        try (final Consumer<byte[], byte[]> testTopicNotMirroredConsumer = primary.kafka().createConsumerAndSubscribeTo(consumerGroupNotMirroredTopicProps, notMirroredTestTopicName)) {
            testTopicNotMirroredConsumer.poll(CONSUMER_POLL_TIMEOUT);
            testTopicNotMirroredConsumer.commitSync();

            // one way replication from primary to backup, add topic filter
            mm2Props.put("offset.lag.max", "0");
            mm2Props.put(PRIMARY_CLUSTER_ALIAS + "->" + BACKUP_CLUSTER_ALIAS + ".topics", ".*");
            mm2Props.put(PRIMARY_CLUSTER_ALIAS + "->" + BACKUP_CLUSTER_ALIAS + ".topics.blacklist", notMirroredTestTopicName);

            mm2Config = new MirrorMakerConfig(mm2Props);

            waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);
            waitUntilMirrorMakerIsRunning(primary, Collections.singletonList(MirrorHeartbeatConnector.class), mm2Config, BACKUP_CLUSTER_ALIAS, PRIMARY_CLUSTER_ALIAS);

            // make sure the topic is not auto-created in the other cluster
            waitForTopicCreated(primary, testTopic1Name);
            waitForTopicCreated(backup, testTopic1Name);
            waitForTopicCreated(primary, notMirroredTestTopicName);
            topicShouldNotBeCreated(backup, notMirroredTestTopicName);

            produceMessages(primaryProducer, notMirroredTestTopicName);

            assertEquals(NUM_RECORDS_PRODUCED, primary.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, notMirroredTestTopicName).count(),
                    "Records were not produced to primary cluster.");

            testTopicNotMirroredConsumer.poll(CONSUMER_POLL_TIMEOUT);
            testTopicNotMirroredConsumer.commitSync();

            // sleep a bit to have MM2 finish offset syncing
            Thread.sleep(TimeUnit.SECONDS.toMillis(2));
            final ConsumerGroupOffsetSyncInspector consumerGroupOffsetSyncInspector = new ConsumerGroupOffsetSyncInspector();
            final Map<SourceAndTarget, ConsumerGroupOffsetsComparer.ConsumerGroupsCompareResult> clusterResults =
                    consumerGroupOffsetSyncInspector.inspect(mm2Props, Duration.ofMinutes(1), Duration.ofSeconds(30),
                            false, true);
            final ConsumerGroupOffsetsComparer.ConsumerGroupsCompareResult result =
                    clusterResults.get(new SourceAndTarget(PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS));
            assertEquals(0, result.getConsumerGroupsCompareResult().stream().filter(element -> element.getGroupId().equals(consumerGroupNotMirroredTopic)).count());
        }
    }
}
