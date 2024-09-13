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

package org.apache.kafka.connect.mirror.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.mirror.MirrorCheckpointConfig;
import org.apache.kafka.connect.mirror.MirrorCheckpointConnector;
import org.apache.kafka.connect.mirror.MirrorClientConfig;
import org.apache.kafka.connect.mirror.MirrorConnectorConfig;
import org.apache.kafka.connect.mirror.MirrorMakerConfig;
import org.apache.kafka.connect.mirror.OffsetSyncStore;
import org.apache.kafka.connect.mirror.ReplicationPolicy;
import org.apache.kafka.connect.mirror.SourceAndTarget;
import org.apache.kafka.connect.mirror.admin.offsetinspector.ConsumerGroupOffsetsComparer;
import org.apache.kafka.connect.mirror.admin.offsetinspector.ConsumerGroupResultsGrouper;
import org.apache.kafka.connect.mirror.admin.offsetinspector.ConsumerGroupsStateCollector;
import org.apache.kafka.connect.mirror.admin.offsetinspector.GroupAndState;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import org.apache.kafka.connect.mirror.admin.offsetinspector.TopicPartitionState;
import org.apache.kafka.connect.mirror.admin.offsetinspector.TopicPartitionStateCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;


public final class ConsumerGroupOffsetSyncInspector {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerGroupOffsetSyncInspector.class);
    private static final String CSV_ROW_FORMAT = "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s";
    private static final String CSV_HEADER_FORMAT = String.format(CSV_ROW_FORMAT,
            "CLUSTER PAIR", "TOPIC", "PARTITION", "GROUP", // unique key
            "SOURCE TOPIC EARLIEST OFFSET", "SOURCE TOPIC LATEST OFFSET", // source topic
            "SOURCE GROUP STATE", "SOURCE GROUP OFFSET", "SOURCE GROUP LAG", // source group
            "TARGET TOPIC EARLIEST OFFSET", "TARGET TOPIC LATEST OFFSET", // target topic
            "TARGET GROUP STATE", "TARGET GROUP OFFSET", "TARGET GROUP LAG", // target group
            "TARGET GROUP LAG TO SOURCE", "SOURCE HAS DATA", "TARGET HAS DATA", "IS OK", "BLOCKING COMPONENT", "MESSAGE" // derived
    );
    private static final String NO_DATA = "-";

    public static void main(final String[] args) throws IOException {
        final ArgumentParser parser = ArgumentParsers.newArgumentParser("mirror-maker-consumer-group-offset-sync-inspector");
        parser.description("MirrorMaker 2.0 consumer group offset sync inspector");
        parser.addArgument("--mm2-config").type(Arguments.fileType().verifyCanRead())
                .metavar("mm2.properties")
                .required(true)
                .help("MM2 configuration file.");

        parser.addArgument("--output-path").type(Arguments.fileType().verifyCanCreate())
                .required(false)
                .help("The result CSV file output path. If not given the result is printed to console.");

        parser.addArgument("--admin-timeout").type((argumentParser, argument, value) -> Duration.parse(value))
                .required(false)
                .setDefault(Duration.ofSeconds(60))
                .help("Kafka API operation timeout in ISO duration format. Defaults to PT1M.");
        parser.addArgument("--request-timeout").type((argumentParser, argument, value) -> Duration.parse(value))
                .required(false)
                .setDefault(Duration.ofSeconds(30))
                .help("Kafka API request timeout in ISO duration format. Defaults to PT30S.");

        parser.addArgument("--include-inactive-groups")
                .required(false)
                .help("Inspect also inactive (empty/dead) consumer groups.")
                .action(Arguments.storeTrue());

        parser.addArgument("--include-ok-groups")
                .required(false)
                .help("Emit consumer group inspection result for groups that are ok.")
                .action(Arguments.storeTrue());

        parser.addArgument("--emit-summary")
                .required(false)
                .help("Emit summary of consumer groups in addition to per-group CSV")
                .action(Arguments.storeTrue());

        final Namespace ns;
        try {
            ns = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            Exit.exit(-1);
            return;
        }

        final File mm2ConfigFile = ns.get("mm2_config");
        final Properties mm2Properties = Utils.loadProps(mm2ConfigFile.getPath());
        final File outputFile = ns.get("output_path");
        final Duration adminTimeout = ns.get("admin_timeout");
        final Duration requestTimeout = ns.get("request_timeout");
        final boolean includeInactiveGroups = ns.getBoolean("include_inactive_groups");
        final boolean includeOkConsumerGroups = ns.getBoolean("include_ok_groups");
        final boolean emitSummary = ns.getBoolean("emit_summary");
        new ConsumerGroupOffsetSyncInspector().run(Utils.propsToStringMap(mm2Properties), outputFile,
                adminTimeout, requestTimeout, includeInactiveGroups, includeOkConsumerGroups, emitSummary);
    }

    public void run(
            final Map<String, String> mm2ConfigProps,
            final File outputFile,
            final Duration adminTimeout,
            final Duration requestTimeout,
            final boolean includeInactiveGroups,
            final boolean includeOkConsumerGroups,
            final boolean emitSummary) throws IOException {
        final Map<SourceAndTarget, ConsumerGroupOffsetsComparer.ConsumerGroupsCompareResult> clusterResults =
                inspect(mm2ConfigProps, adminTimeout, requestTimeout, includeInactiveGroups, includeOkConsumerGroups);
        LOGGER.info("Writing result CSV to {}", outputFile != null ? outputFile.getPath() : "STDOUT");
        if (outputFile == null) {
            writeToOutputStream(System.out, clusterResults);
        } else {
            try (PrintStream out = new PrintStream(Files.newOutputStream(outputFile.toPath()),
                    false, StandardCharsets.UTF_8.name())) {
                writeToOutputStream(out, clusterResults);
            }
        }
        if (emitSummary) {
            ConsumerGroupResultsGrouper grouper = new ConsumerGroupResultsGrouper();
            clusterResults.values()
                    .stream()
                    .flatMap(results -> results.getConsumerGroupsCompareResult().stream())
                    .forEach(grouper);
            grouper.writeToOutputStream(System.out);
        }
        LOGGER.info("Done.");
    }

    public Map<SourceAndTarget, ConsumerGroupOffsetsComparer.ConsumerGroupsCompareResult> inspect(
            final Map<String, String> mm2ConfigProps,
            final Duration adminTimeout,
            final Duration requestTimeout,
            final boolean includeInactiveGroups,
            final boolean includeOkConsumerGroups
    ) {
        final MirrorMakerConfig mm2Config = new MirrorMakerConfig(mm2ConfigProps);
        final Map<SourceAndTarget, ConsumerGroupOffsetsComparer.ConsumerGroupsCompareResult> clusterResults = new HashMap<>();

        mm2Config.clusterPairs().forEach(sourceAndTarget -> {
            if (!mm2Config.clusterPairEnabled(sourceAndTarget)) {
                // If not enabled, do not inspect.
                return;
            }
            final MirrorCheckpointConfig mirrorCheckpointConfig = new MirrorCheckpointConfig(
                    mm2Config.connectorBaseConfig(sourceAndTarget, MirrorCheckpointConnector.class)
            );
            if (!mirrorCheckpointConfig.getBoolean("enabled") ||
                    mirrorCheckpointConfig.emitCheckpointsInterval().isNegative()) {
                // If checkpoint connector is not enabled or emit of checkpoints is disabled (negative duration),
                // do not inspect.
                return;
            }

            // Start the offset sync store loading, use barrier to stop the result comparison to run before
            // offset sync store is ready.
            final OffsetSyncStore offsetSyncStore = new OffsetSyncStore(mirrorCheckpointConfig);
            final ExecutorService executor = Executors.newSingleThreadExecutor();
            final Future<?> offsetSyncStoreLoadFuture = executor.submit(() -> {
                offsetSyncStore.start(true);
            });

            final String sourceClusterAlias = sourceAndTarget.source();
            final String targetClusterAlias = sourceAndTarget.target();

            final MirrorClientConfig sourceMirrorClientConfig = mm2Config.clientConfig(sourceClusterAlias);
            final Map<String, Object> sourceAdminConfig = sourceMirrorClientConfig.adminConfig();
            sourceAdminConfig.put("default.api.timeout.ms", Math.toIntExact(adminTimeout.toMillis()));
            sourceAdminConfig.put("request.timeout.ms", Math.toIntExact(requestTimeout.toMillis()));
            final AdminClient sourceAdminClient = KafkaAdminClient.create(sourceAdminConfig);
            final MirrorClientConfig targetMirrorClientConfig = mm2Config.clientConfig(targetClusterAlias);
            final Map<String, Object> targetAdminConfig = targetMirrorClientConfig.adminConfig();
            targetAdminConfig.put("default.api.timeout.ms", Math.toIntExact(adminTimeout.toMillis()));
            targetAdminConfig.put("request.timeout.ms", Math.toIntExact(requestTimeout.toMillis()));
            final AdminClient targetAdminClient = KafkaAdminClient.create(targetAdminConfig);

            final ConsumerGroupsStateCollector sourceCollector = ConsumerGroupsStateCollector.builder()
                    .withAdminTimeout(adminTimeout)
                    .withAdminClient(sourceAdminClient)
                    .withMirrorCheckpointConfig(mirrorCheckpointConfig)
                    .includeInactiveGroups(includeInactiveGroups)
                    .build();
            final Map<GroupAndState, Map<TopicPartition, OffsetAndMetadata>> sourceConsumerOffsets = sourceCollector.collectConsumerGroupsState();

            final ConsumerGroupsStateCollector targetCollector = ConsumerGroupsStateCollector.builder()
                    .withAdminTimeout(adminTimeout)
                    .withAdminClient(targetAdminClient)
                    .withMirrorCheckpointConfig(mirrorCheckpointConfig)
                    .build();
            final Map<GroupAndState, Map<TopicPartition, OffsetAndMetadata>> targetConsumerOffsets = targetCollector.getCommittedOffsets(sourceConsumerOffsets.keySet());

            Set<TopicPartition> sourceTopicPartitions = sourceConsumerOffsets.values().stream().flatMap(map -> map.keySet().stream()).collect(Collectors.toSet());
            Map<TopicPartition, TopicPartition> sourceToTargetPartition = mapToRemoteTopics(mirrorCheckpointConfig, sourceTopicPartitions);

            final Map<TopicPartition, TopicPartitionState> sourceTopicStates = TopicPartitionStateCollector.builder()
                    .withAdminTimeout(adminTimeout)
                    .withAdminClient(sourceAdminClient)
                    .withTopicPartitions(sourceTopicPartitions)
                    .build()
                    .getTopicStates();

            final Map<TopicPartition, TopicPartitionState> targetTopicStates = TopicPartitionStateCollector.builder()
                    .withAdminTimeout(adminTimeout)
                    .withAdminClient(targetAdminClient)
                    .withTopicPartitions(new HashSet<>(sourceToTargetPartition.values()))
                    .build()
                    .getTopicStates();


            // No more new tasks allowed.
            executor.shutdown();
            try {
                offsetSyncStoreLoadFuture.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }

            final ConsumerGroupOffsetsComparer comparer = ConsumerGroupOffsetsComparer.builder()
                    .withTopicMappings(sourceToTargetPartition)
                    .withSourceTopics(sourceTopicStates)
                    .withTargetTopics(targetTopicStates)
                    .withSourceConsumerOffsets(sourceConsumerOffsets)
                    .withTargetConsumerOffsets(targetConsumerOffsets)
                    .withIncludeOkConsumerGroups(includeOkConsumerGroups)
                    .withOffsetSyncStore(offsetSyncStore)
                    .build();
            final ConsumerGroupOffsetsComparer.ConsumerGroupsCompareResult result = comparer.compare();
            clusterResults.put(sourceAndTarget, result);
            sourceAdminClient.close();
            targetAdminClient.close();
            offsetSyncStore.close();
            try {
                if (!executor.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
            }
        });
        return clusterResults;
    }

    private static Map<TopicPartition, TopicPartition> mapToRemoteTopics(MirrorConnectorConfig config, Set<TopicPartition> sourcePartitions) {
        ReplicationPolicy replicationPolicy = config.replicationPolicy();
        try {
            return sourcePartitions.stream()
                    .collect(Collectors.toMap(Function.identity(), tp -> new TopicPartition(replicationPolicy.formatRemoteTopic(config.sourceClusterAlias(), tp.topic()), tp.partition())));
        } finally {
            Utils.maybeCloseQuietly(replicationPolicy, "replication policy");
        }
    }

    private void writeToOutputStream(final PrintStream out,
                                     final Map<SourceAndTarget, ConsumerGroupOffsetsComparer.ConsumerGroupsCompareResult> clusterResults) {
        try {
            out.write(CSV_HEADER_FORMAT.getBytes(StandardCharsets.UTF_8));
            out.write("\n".getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        clusterResults.forEach((sourceAndTarget, allResults) -> allResults.getConsumerGroupsCompareResult().stream().sorted().forEach(result -> {
            try {
                out.write(String.format(CSV_ROW_FORMAT,
                        // unique key
                        sourceAndTarget.toString(),
                        result.getTopic(),
                        result.getPartition(),
                        result.getGroupId(),
                        // source topic
                        result.getSourceEarliest(),
                        result.getSourceLatest(),
                        // source group
                        result.getGroupState(),
                        Objects.toString(result.getSourceOffset(), NO_DATA),
                        Objects.toString(result.getSourceLag(), NO_DATA),
                        // target topic
                        Objects.toString(result.getTargetEarliest(), NO_DATA),
                        Objects.toString(result.getTargetLatest(), NO_DATA),
                        // target group
                        Objects.toString(result.getTargetGroupState(), NO_DATA),
                        Objects.toString(result.getTargetOffset(), NO_DATA),
                        Objects.toString(result.getTargetLag(), NO_DATA),
                        // derived
                        Objects.toString(result.getLagAtTargetToSource(), NO_DATA),
                        result.sourceHasData(),
                        result.targetHasData(),
                        result.isOk(),
                        Objects.toString(result.blockingComponent(), NO_DATA),
                        result.getMessage()
                ).getBytes(StandardCharsets.UTF_8));
                out.write("\n".getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }));
        out.flush();
    }
}
