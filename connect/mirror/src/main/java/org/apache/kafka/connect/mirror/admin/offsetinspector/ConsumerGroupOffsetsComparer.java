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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.mirror.OffsetSync;
import org.apache.kafka.connect.mirror.OffsetSyncStore;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public final class ConsumerGroupOffsetsComparer {

    public static final String SUCCESS_MESSAGE = "Successfully synced, operating normally";
    public static final String NOT_SYNCED_EMPTY_PARTITION_OTHER_SUCCESSFUL_MESSAGE = "Intentionally not synced because source partition is empty. Other partitions in this group are synced.";
    public static final String ERR_NOT_SYNCED_OTHER_SUCCESSFUL_MESSAGE = "Erroneously not synced. Source partition has data, and other partitions for this group are synced.";
    private static final String NOT_SYNCED_GROUP_TOO_OLD_OTHER_SUCCESSFUL_MESSAGE = "Intentionally not synced because source group is too old. Other partitions in this group are synced.";
    public static final String NOT_SYNCED_EMPTY_PARTITION_ALL_NOT_SYNCED_MESSAGE = "Intentionally not synced because source partition is empty. Other partitions in this group are not synced.";
    public static final String ERR_NOT_SYNCED_ALL_NOT_SYNCED_MESSAGE = "Erroneously not synced. Source partition has data, and other partitions for this group are not synced.";
    private static final String NOT_SYNCED_GROUP_TOO_OLD_ALL_NOT_SYNCED_MESSAGE = "Intentionally not synced because source group is too old. Other partitions in this group are not synced.";

    private final Map<TopicPartition, TopicPartition> sourceToTopicPartition;
    private final Map<TopicPartition, TopicPartitionState> sourceTopics;
    private final Map<TopicPartition, TopicPartitionState> targetTopics;
    private final Map<GroupAndState, Map<TopicPartition, OffsetAndMetadata>> sourceConsumerOffsets;
    private final Map<GroupAndState, Map<TopicPartition, OffsetAndMetadata>> targetConsumerOffsets = new HashMap<>();
    private final boolean includeOkConsumerGroups;
    private final OffsetSyncStore offsetSyncStore;

    public ConsumerGroupOffsetsComparer(final Map<TopicPartition, TopicPartition> sourceToTargetPartition,
                                        final Map<TopicPartition, TopicPartitionState> sourceTopics,
                                        final Map<TopicPartition, TopicPartitionState> targetTopics,
                                        final Map<GroupAndState, Map<TopicPartition, OffsetAndMetadata>> sourceConsumerOffsets,
                                        final Map<GroupAndState, Map<TopicPartition, OffsetAndMetadata>> targetConsumerOffsets,
                                        final boolean includeOkConsumerGroups, final OffsetSyncStore offsetSyncStore) {
        this.sourceToTopicPartition = sourceToTargetPartition;
        this.sourceTopics = sourceTopics;
        this.targetTopics = targetTopics;
        this.targetConsumerOffsets.putAll(Objects.requireNonNull(targetConsumerOffsets));
        this.sourceConsumerOffsets = Collections.unmodifiableMap(Objects.requireNonNull(sourceConsumerOffsets));
        this.includeOkConsumerGroups = includeOkConsumerGroups;
        this.offsetSyncStore = offsetSyncStore;
    }

    public ConsumerGroupsCompareResult compare() {
        final ConsumerGroupsCompareResult result = new ConsumerGroupsCompareResult();

        sourceConsumerOffsets.forEach((groupAndState, sourceOffsetData) -> {
            final Optional<Map<TopicPartition, OffsetAndMetadata>> maybeTargetGroupConsumerOffsets =
                    Optional.ofNullable(targetConsumerOffsets.remove(groupAndState));
            if (maybeTargetGroupConsumerOffsets.isPresent()) {
                // Target group has at least one partition synced
                final Map<TopicPartition, OffsetAndMetadata> targetOffsetData = maybeTargetGroupConsumerOffsets.get();
                sourceOffsetData.forEach((sourceTopicPartition, metadata) -> {
                    final Optional<OffsetAndMetadata> maybeTargetOffsetAndMetadata = Optional
                            .ofNullable(targetOffsetData.get(sourceTopicPartition));
                    final TopicPartition targetTopicPartition = sourceToTopicPartition.get(sourceTopicPartition);
                    final TopicPartitionState sourceTopicState = sourceTopics.getOrDefault(sourceTopicPartition, TopicPartitionState.EMPTY);
                    final TopicPartitionState targetTopicState = targetTopics.getOrDefault(targetTopicPartition, TopicPartitionState.EMPTY);
                    final boolean targetOk;
                    final String message;
                    final String blockingComponent;
                    if (maybeTargetOffsetAndMetadata.isPresent()) {
                        // Target partition has offset
                        final Long targetOffset = maybeTargetOffsetAndMetadata
                                .get().offset();
                        final Long targetLag;
                        final Long lagAtTargetToSource;
                        // If target lag is negative, round back to 0.
                        final Long targetLatestOffset = targetTopicState.latestOffset();
                        targetLag =  targetLatestOffset == null ? null :
                                (targetLatestOffset >= targetOffset ? targetLatestOffset - targetOffset : 0);

                        final Long sourceLatestOffset = sourceTopicState.latestOffset();
                        if (sourceLatestOffset != null) {
                            final Optional<OffsetSync> maybeOffsetSync = offsetSyncStore.latestOffsetSync(sourceTopicPartition, sourceLatestOffset);
                            if (maybeOffsetSync.isPresent()) {
                                final OffsetSync offsetSync = maybeOffsetSync.get();
                                final long sourceAndTargetOffsetDifference = offsetSync.upstreamOffset() - offsetSync.downstreamOffset();
                                lagAtTargetToSource = sourceLatestOffset - (targetOffset + sourceAndTargetOffsetDifference);
                            } else {
                                // no offset for downstream
                                lagAtTargetToSource = null;
                            }
                        } else {
                            // no offset for upstream
                            lagAtTargetToSource = null;
                        }

                        // Target offset present, assume ok sync.
                        message = SUCCESS_MESSAGE;
                        if (includeOkConsumerGroups) {
                            result.addConsumerGroupCompareResult(new ConsumerGroupCompareResult(groupAndState,
                                    sourceTopicPartition, sourceTopicState, targetTopicState, metadata.offset(), lagAtTargetToSource, targetOffset, targetLag, true, null, message));
                        }
                    } else {
                        // If no target offset, check if source partition is empty.
                        // If source partition is empty Mirrormaker does not sync offset.
                        final boolean isEmpty = sourceTopicState.isEmpty();
                        // Also check to see if the source group is earlier than any offset in the topic
                        // MM2 doesn't translate consumer groups that are older than all the data.
                        final boolean sourceGroupValid = sourceTopicState.contains(metadata.offset());
                        if (isEmpty) {
                            targetOk = true;
                            blockingComponent = "SOURCE PARTITION";
                            message = NOT_SYNCED_EMPTY_PARTITION_OTHER_SUCCESSFUL_MESSAGE;
                        } else if (sourceGroupValid) {
                            targetOk = false;
                            blockingComponent = "REPLICATION";
                            message = ERR_NOT_SYNCED_OTHER_SUCCESSFUL_MESSAGE;
                        } else {
                            targetOk = true;
                            blockingComponent = "SOURCE GROUP";
                            message = NOT_SYNCED_GROUP_TOO_OLD_OTHER_SUCCESSFUL_MESSAGE;
                        }
                        if (includeOkConsumerGroups || !targetOk) {
                            result.addConsumerGroupCompareResult(
                                    new ConsumerGroupCompareResult(groupAndState,
                                            sourceTopicPartition, sourceTopicState, targetTopicState, metadata.offset(), null, null, null, targetOk, blockingComponent, message));
                        }
                    }
                });
            } else {
                // No group at target, add each partition to result without target offset.
                sourceOffsetData.forEach((sourceTopicPartition, metadata) -> {
                    final TopicPartition targetTopicPartition = sourceToTopicPartition.get(sourceTopicPartition);
                    final TopicPartitionState sourceTopicState = sourceTopics.getOrDefault(sourceTopicPartition, TopicPartitionState.EMPTY);
                    final TopicPartitionState targetTopicState = targetTopics.getOrDefault(targetTopicPartition, TopicPartitionState.EMPTY);
                    final boolean targetOk;
                    final String message;
                    final String blockingComponent;
                    final boolean isEmpty = sourceTopicState.isEmpty();
                    final boolean sourceGroupValid = sourceTopicState.contains(metadata.offset());
                    if (isEmpty) {
                        targetOk = true;
                        blockingComponent = "SOURCE PARTITION";
                        message = NOT_SYNCED_EMPTY_PARTITION_ALL_NOT_SYNCED_MESSAGE;
                    } else if (sourceGroupValid) {
                        targetOk = false;
                        blockingComponent = "REPLICATION";
                        message = ERR_NOT_SYNCED_ALL_NOT_SYNCED_MESSAGE;
                    } else {
                        targetOk = true;
                        blockingComponent = "SOURCE GROUP";
                        message = NOT_SYNCED_GROUP_TOO_OLD_ALL_NOT_SYNCED_MESSAGE;
                    }
                    if (includeOkConsumerGroups || !targetOk) {
                        result.addConsumerGroupCompareResult(new ConsumerGroupCompareResult(groupAndState, sourceTopicPartition,
                                sourceTopicState, targetTopicState, metadata.offset(), null, null, null, targetOk, blockingComponent, message));
                    }
                });
            }
        });

        // Target consumer offsets map is mutated by remove calls and leaves behind extra groups at target.
        result.setExtraAtTarget(targetConsumerOffsets);
        return result;
    }

    public static ConsumerGroupOffsetsComparerBuilder builder() {
        return new ConsumerGroupOffsetsComparerBuilder();
    }

    public static final class ConsumerGroupsCompareResult {

        private Map<GroupAndState, Map<TopicPartition, OffsetAndMetadata>> extraAtTarget = Collections.emptyMap();
        private final Set<ConsumerGroupCompareResult> result = new HashSet<>();

        private ConsumerGroupsCompareResult() {
            /* hide constructor */
        }

        private void addConsumerGroupCompareResult(final ConsumerGroupCompareResult groupResult) {
            result.add(groupResult);
        }

        private void setExtraAtTarget(final Map<GroupAndState, Map<TopicPartition, OffsetAndMetadata>> extraAtTarget) {
            this.extraAtTarget = extraAtTarget;
        }

        public Set<ConsumerGroupCompareResult> getConsumerGroupsCompareResult() {
            return result;
        }

        public boolean isEmpty() {
            return result.isEmpty();
        }


        @Override
        public String toString() {
            return "ConsumerGroupsCompareResult{" + "extraAtTarget=" + extraAtTarget + ", result=" + result + '}';
        }
    }

    public static final class ConsumerGroupCompareResult implements Comparable<ConsumerGroupCompareResult> {
        private static final Comparator<ConsumerGroupCompareResult> COMPARATOR = Comparator
                .comparing(ConsumerGroupCompareResult::getGroupId, String.CASE_INSENSITIVE_ORDER)
                .thenComparing(ConsumerGroupCompareResult::getTopic, String.CASE_INSENSITIVE_ORDER)
                .thenComparing(ConsumerGroupCompareResult::getPartition);
        private final GroupAndState sourceGroup;
        private final GroupAndState targetGroup;
        private final TopicPartition topicPartition;
        private final TopicPartitionState sourceTopicState;
        private final TopicPartitionState targetTopicState;
        private final Long sourceOffset;
        private final Long lagAtTargetToSource;
        private final Long targetOffset;
        private final Long targetLag;
        private final boolean groupStateOk;
        private final String blockingComponent;
        private final String message;

        public ConsumerGroupCompareResult(final GroupAndState groupId, final TopicPartition topicPartition,
                                          final TopicPartitionState sourceTopicState,
                                          final TopicPartitionState targetTopicState,
                                          final Long sourceOffset, final Long lagAtTargetToSource,
                                          final Long targetOffset, final Long targetLag,
                                          final boolean groupStateOk, final String blockingComponent, final String message) {
            this.sourceGroup = Objects.requireNonNull(groupId);
            this.targetGroup = new GroupAndState(groupId.id(), ConsumerGroupState.UNKNOWN);
            this.topicPartition = topicPartition;
            this.sourceTopicState = sourceTopicState;
            this.targetTopicState = targetTopicState;
            this.sourceOffset = sourceOffset;
            this.lagAtTargetToSource = lagAtTargetToSource;
            this.targetOffset = targetOffset;
            this.targetLag = targetLag;
            this.groupStateOk = groupStateOk;
            this.blockingComponent = blockingComponent;
            this.message = message;
        }

        public Long getSourceEarliest() {
            return sourceTopicState.earliestOffset();
        }

        public Long getSourceLatest() {
            return sourceTopicState.latestOffset();
        }

        public boolean sourceHasData() {
            return !sourceTopicState.isEmpty();
        }

        public Long getTargetEarliest() {
            return targetTopicState.earliestOffset();
        }

        public Long getTargetLatest() {
            return targetTopicState.latestOffset();
        }

        public boolean targetHasData() {
            return !targetTopicState.isEmpty();
        }

        public String getGroupId() {
            return sourceGroup.id();
        }

        public ConsumerGroupState getGroupState() {
            return sourceGroup.state();
        }

        public ConsumerGroupState getTargetGroupState() {
            return targetGroup.state();
        }

        public String getTopic() {
            return this.topicPartition.topic();
        }

        public int getPartition() {
            return this.topicPartition.partition();
        }

        public Long getSourceOffset() {
            return sourceOffset;
        }

        public Long getSourceLag() {
            Long topicLatest = getSourceLatest();
            Long groupOffset = getSourceOffset();
            return (topicLatest == null || groupOffset == null) ? null : (topicLatest - groupOffset);
        }

        public boolean sourceGroupInTopic() {
            Long topicEarliest = getSourceEarliest();
            Long topicLatest = getSourceLatest();
            Long groupOffset = getSourceOffset();
            return topicEarliest != null && topicLatest != null && groupOffset != null
                    && topicEarliest <= groupOffset && groupOffset <= topicLatest;
        }

        public Long getLagAtTargetToSource() {
            return lagAtTargetToSource;
        }

        public Long getTargetOffset() {
            return targetOffset;
        }

        public Long getTargetLag() {
            return targetLag;
        }

        public boolean isOk() {
            return groupStateOk;
        }

        public String blockingComponent() {
            return blockingComponent;
        }

        public String getMessage() {
            return message;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ConsumerGroupCompareResult result = (ConsumerGroupCompareResult) o;
            return groupStateOk == result.groupStateOk
                    && Objects.equals(sourceGroup, result.sourceGroup)
                    && Objects.equals(topicPartition, result.topicPartition)
                    && Objects.equals(sourceOffset, result.sourceOffset)
                    && Objects.equals(lagAtTargetToSource, result.lagAtTargetToSource)
                    && Objects.equals(targetOffset, result.targetOffset)
                    && Objects.equals(targetLag, result.targetLag)
                    && Objects.equals(message, result.message);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceGroup, topicPartition, sourceOffset, lagAtTargetToSource,
                    targetOffset, targetLag, groupStateOk, message);
        }

        @Override
        public String toString() {
            return "ConsumerGroupCompareResult{" +
                    "groupAndState=" + sourceGroup +
                    ", topicPartition=" + topicPartition +
                    ", sourceOffset=" + sourceOffset +
                    ", lagAtTargetToSource=" + lagAtTargetToSource +
                    ", targetOffset=" + targetOffset +
                    ", targetLag=" + targetLag +
                    ", groupStateOk=" + groupStateOk +
                    ", message='" + message + '\'' +
                    '}';
        }

        @Override
        public int compareTo(final ConsumerGroupCompareResult other) {
            return COMPARATOR.compare(this, other);
        }
    }

    public static final class ConsumerGroupOffsetsComparerBuilder {

        private Map<TopicPartition, TopicPartition> sourceToTargetPartition;
        private final Map<TopicPartition, TopicPartitionState> sourceTopics = new HashMap<>();
        private final Map<TopicPartition, TopicPartitionState> targetTopics = new HashMap<>();
        private final Map<GroupAndState, Map<TopicPartition, OffsetAndMetadata>> sourceConsumerOffsets = new HashMap<>();
        private final Map<GroupAndState, Map<TopicPartition, OffsetAndMetadata>> targetConsumerOffsets = new HashMap<>();
        private boolean withIncludeOkConsumerGroups = false;
        private OffsetSyncStore offsetSyncStore;

        private ConsumerGroupOffsetsComparerBuilder() {
            /* hide constructor */
        }

        public ConsumerGroupOffsetsComparerBuilder withTopicMappings(Map<TopicPartition, TopicPartition> sourceToTargetPartition) {
            this.sourceToTargetPartition = sourceToTargetPartition;
            return this;
        }

        public ConsumerGroupOffsetsComparerBuilder withSourceTopics(Map<TopicPartition, TopicPartitionState> sourceTopics) {
            this.sourceTopics.putAll(sourceTopics);
            return this;
        }

        public ConsumerGroupOffsetsComparerBuilder withTargetTopics(Map<TopicPartition, TopicPartitionState> targetTopics) {
            this.targetTopics.putAll(targetTopics);
            return this;
        }

        public ConsumerGroupOffsetsComparerBuilder withSourceConsumerOffsets(
                final Map<GroupAndState, Map<TopicPartition, OffsetAndMetadata>> sourceConsumerOffsets) {
            this.sourceConsumerOffsets.putAll(Objects.requireNonNull(sourceConsumerOffsets));
            return this;
        }

        public ConsumerGroupOffsetsComparerBuilder withTargetConsumerOffsets(
                final Map<GroupAndState, Map<TopicPartition, OffsetAndMetadata>> targetConsumerOffsets) {
            this.targetConsumerOffsets.putAll(Objects.requireNonNull(targetConsumerOffsets));
            return this;
        }

        public ConsumerGroupOffsetsComparerBuilder withIncludeOkConsumerGroups(final boolean includeOkConsumerGroups) {
            this.withIncludeOkConsumerGroups = includeOkConsumerGroups;
            return this;
        }

        public ConsumerGroupOffsetsComparerBuilder withOffsetSyncStore(final OffsetSyncStore offsetSyncStore) {
            this.offsetSyncStore = offsetSyncStore;
            return this;
        }

        public ConsumerGroupOffsetsComparer build() {
            return new ConsumerGroupOffsetsComparer(sourceToTargetPartition, sourceTopics, targetTopics,
                    sourceConsumerOffsets, targetConsumerOffsets, withIncludeOkConsumerGroups, offsetSyncStore);
        }
    }
}
