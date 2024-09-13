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

import org.apache.kafka.common.ConsumerGroupState;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

public class ConsumerGroupResultsGrouper implements Consumer<ConsumerGroupOffsetsComparer.ConsumerGroupCompareResult> {

    private final Map<ResultsGroup, Integer> aggregate = new HashMap<>();

    @Override
    public void accept(ConsumerGroupOffsetsComparer.ConsumerGroupCompareResult result) {
        aggregate.compute(ResultsGroup.ofResult(result), (ignored, previous) -> previous == null ? 1 : previous + 1);
    }

    public void writeToOutputStream(PrintStream out) {
        aggregate.forEach((group, count) -> out.printf(
                "%s partitions in state: Source has data: %s, Source group: %s, Target has data: %s, Target group: %s," +
                        " ok: %s, Blocking component: %s, Message: %s%n",
                count,
                group.sourcePartitionData, group.sourceGroupState,
                group.targetPartitionData, group.targetGroupState,
                group.isOk, group.blockingComponent, group.message));
    }

    public static class ResultsGroup {

        private final boolean sourcePartitionData;
        private final ConsumerGroupState sourceGroupState;
        private final boolean targetPartitionData;
        private final ConsumerGroupState targetGroupState;
        private final boolean isOk;
        private final String blockingComponent;
        private final String message;

        private ResultsGroup(
                boolean sourcePartitionData,
                ConsumerGroupState sourceGroupState,
                boolean targetPartitionData,
                ConsumerGroupState targetGroupState,
                boolean isOk, String blockingComponent,
                String message
        ) {
            this.sourcePartitionData = sourcePartitionData;
            this.sourceGroupState = sourceGroupState;
            this.targetPartitionData = targetPartitionData;
            this.targetGroupState = targetGroupState;
            this.isOk = isOk;
            this.blockingComponent = blockingComponent;
            this.message = message;
        }

        private static ResultsGroup ofResult(ConsumerGroupOffsetsComparer.ConsumerGroupCompareResult result) {
            return new ResultsGroup(result.sourceHasData(), result.getGroupState(), result.targetHasData(),
                    result.getTargetGroupState(), result.isOk(), result.blockingComponent(), result.getMessage());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ResultsGroup that = (ResultsGroup) o;
            return sourcePartitionData == that.sourcePartitionData
                    && targetPartitionData == that.targetPartitionData
                    && isOk == that.isOk
                    && sourceGroupState == that.sourceGroupState
                    && targetGroupState == that.targetGroupState
                    && Objects.equals(blockingComponent, that.blockingComponent)
                    && Objects.equals(message, that.message);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourcePartitionData, sourceGroupState, targetPartitionData,
                    targetGroupState, isOk, blockingComponent, message);
        }
    }

}
