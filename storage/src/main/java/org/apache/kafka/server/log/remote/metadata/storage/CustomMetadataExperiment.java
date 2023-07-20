package org.apache.kafka.server.log.remote.metadata.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataSerde;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata.CustomMetadata;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class CustomMetadataExperiment {
    public static void main(String[] args) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);

        JsonNodeFactory jsonNodeFactory = JsonNodeFactory.instance;
        ArrayNode cases = jsonNodeFactory.arrayNode();
        RemoteLogMetadataSerde serde = new RemoteLogMetadataSerde();

//        CustomMetadata customMetadata = createCustomMetadata();

        ObjectNode caseNode = jsonNodeFactory.objectNode();
        cases.add(caseNode);
        caseNode.put("name", "simple");

        Uuid topicId = Uuid.fromString("DK7IDcpaTWGCjkd0w54QYw");
        String topic = "topic1";
        int partition = 42;
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        TopicIdPartition topicIdPartition = new TopicIdPartition(topicId, topicPartition);
        Uuid segmentId = Uuid.fromString("K6pI3ntkQguqSTUNJRTa4Q");
        RemoteLogSegmentId remoteLogSegmentId = new RemoteLogSegmentId(topicIdPartition, segmentId);
        long startOffset = 123L;
        long endOffset = 321L;
        long maxTimestampMs = 100124L;
        int brokerId = 0;
        long eventTimestampMs = 1951929L;
        int segmentSizeInBytes = 6942;
        Map<Integer, Long> segmentLeaderEpochs = new HashMap<>();
        segmentLeaderEpochs.put(0, 0L);
        segmentLeaderEpochs.put(1, 20L);
        segmentLeaderEpochs.put(2, 50L);
        segmentLeaderEpochs.put(3, 80L);

        RemoteLogSegmentMetadata remoteLogSegmentMetadata = new RemoteLogSegmentMetadata(
                remoteLogSegmentId, startOffset, endOffset, maxTimestampMs, brokerId, eventTimestampMs, segmentSizeInBytes, segmentLeaderEpochs);

        ObjectNode original = jsonNodeFactory.objectNode();
        original.put("topicId", remoteLogSegmentMetadata.topicIdPartition().topicId().toString());
        original.put("topic", remoteLogSegmentMetadata.topicIdPartition().topic());
        original.put("partition", remoteLogSegmentMetadata.topicIdPartition().partition());
        original.put("segmentId", remoteLogSegmentMetadata.remoteLogSegmentId().id().toString());
        original.put("startOffset", remoteLogSegmentMetadata.startOffset());
        original.put("endOffset", remoteLogSegmentMetadata.endOffset());
        original.put("maxTimestampMs", remoteLogSegmentMetadata.maxTimestampMs());
        original.put("brokerId", remoteLogSegmentMetadata.brokerId());
        original.put("eventTimestampMs", remoteLogSegmentMetadata.eventTimestampMs());
        original.put("segmentSizeInBytes", remoteLogSegmentMetadata.segmentSizeInBytes());
        original.set("segmentLeaderEpochs", mapper.convertValue(remoteLogSegmentMetadata.segmentLeaderEpochs(), ObjectNode.class));
        if (remoteLogSegmentMetadata.customMetadata().isPresent()) {
            original.put("customMetadata", remoteLogSegmentMetadata.customMetadata().get().value());
        } else {
            original.set("customMetadata", null);
        }
        original.put("state", remoteLogSegmentMetadata.state().id());

        caseNode.set("original", original);

        byte[] serialized = serde.serialize(remoteLogSegmentMetadata);
        caseNode.put("serialized", serialized);
        System.out.println(mapper.writeValueAsString(cases));
    }

    private static CustomMetadata createCustomMetadata() {
        Schema schema = new Schema(Field.TaggedFieldsSection.of(
                0, new Field("remote_size", Type.VARLONG)
        ));
        Struct struct = new Struct(schema);
        TreeMap<Integer, Object> taggedFields = new TreeMap<>();
        taggedFields.put(0, 123L);
        struct.set("_tagged_fields", taggedFields);

        int size = struct.sizeOf();
        System.out.println(size);
        ByteBuffer buf = ByteBuffer.allocate(size);
        struct.writeTo(buf);

        return new CustomMetadata(buf.array());
    }
}
