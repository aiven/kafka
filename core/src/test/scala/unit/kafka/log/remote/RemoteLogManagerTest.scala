/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.log.remote

import kafka.cluster.Partition

import kafka.log._
import kafka.server._
import kafka.server.checkpoints.LeaderEpochCheckpoint
import kafka.server.epoch.{EpochEntry, LeaderEpochFileCache}
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType
import org.apache.kafka.server.log.remote.storage._
import org.mockito.Mockito.{mock, reset, when}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.{AfterEach, Test}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, anyInt, anyLong}
import org.mockito.invocation.InvocationOnMock

import java.io.{File, FileInputStream}
import java.nio.file.{Files, Paths}
import java.util.{Collections, Optional, Properties}
import java.{lang, util}
import scala.collection.Seq

class RemoteLogManagerTest {

  val clusterId = "test-cluster-id"
  val brokerId = 0
  val topicPartition = new TopicPartition("test-topic", 0)
  val topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), topicPartition)
  val time = new MockTime()
  val brokerTopicStats = new BrokerTopicStats
  val logsDir: String = Files.createTempDirectory("kafka-").toString
  val checkpoint: LeaderEpochCheckpoint = new LeaderEpochCheckpoint {
    private var epochs: Seq[EpochEntry] = Seq()
    override def write(epochs: Iterable[EpochEntry]): Unit = this.epochs = epochs.toSeq
    override def read(): Seq[EpochEntry] = this.epochs
  }
  val cache = new LeaderEpochFileCache(topicPartition, checkpoint)
  val rlmConfig: RemoteLogManagerConfig = createRLMConfig()

  @AfterEach
  def afterEach(): Unit = {
    Utils.delete(Paths.get(logsDir).toFile)
  }

  @Test
  def testRLMConfig(): Unit = {
    val key = "hello"
    val rlmmConfigPrefix = RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX
    val props: Properties = new Properties()
    props.put(rlmmConfigPrefix + key, "world")
    props.put("remote.log.metadata.y", "z")

    val rlmConfig = createRLMConfig(props)
    val rlmmConfig = rlmConfig.remoteLogMetadataManagerProps()
    assertEquals(props.get(rlmmConfigPrefix + key), rlmmConfig.get(key))
    assertFalse(rlmmConfig.containsKey("remote.log.metadata.y"))
  }

  @Test
  def testFindHighestRemoteOffsetOnEmptyRemoteStorage(): Unit = {
    cache.assign(0, 0)
    cache.assign(1, 500)

    val log: UnifiedLog = mock(classOf[UnifiedLog])
    when(log.leaderEpochCache).thenReturn(Option(cache))

    val rlmmManager: RemoteLogMetadataManager = mock(classOf[RemoteLogMetadataManager])
    when(rlmmManager.highestOffsetForEpoch(ArgumentMatchers.eq(topicIdPartition), anyInt()))
      .thenReturn(Optional.empty(): Optional[java.lang.Long])

    val remoteLogManager = new RemoteLogManager(_ => Option(log),
      (_, _) => {}, rlmConfig, time, 1, clusterId, logsDir, brokerTopicStats) {
      override private[remote] def createRemoteLogMetadataManager(): RemoteLogMetadataManager = rlmmManager
    }
    assertEquals(-1L, remoteLogManager.findHighestRemoteOffset(topicIdPartition))
  }

  @Test
  def testFindHighestRemoteOffset(): Unit = {
    cache.assign(0, 0)
    cache.assign(1, 500)

    val log: UnifiedLog = mock(classOf[UnifiedLog])
    when(log.leaderEpochCache).thenReturn(Option(cache))

    val rlmmManager: RemoteLogMetadataManager = mock(classOf[RemoteLogMetadataManager])
    when(rlmmManager.highestOffsetForEpoch(ArgumentMatchers.eq(topicIdPartition), anyInt())).thenAnswer((invocation: InvocationOnMock) => {
      val epoch = invocation.getArgument[Int](1)
      if (epoch == 0) Optional.of(200L) else Optional.empty()
    })

    val remoteLogManager = new RemoteLogManager(_ => Option(log),
      (_, _) => {}, rlmConfig, time, 1, clusterId, logsDir, brokerTopicStats) {
      override private[remote] def createRemoteLogMetadataManager(): RemoteLogMetadataManager = rlmmManager
    }
    assertEquals(200L, remoteLogManager.findHighestRemoteOffset(topicIdPartition))
  }

  @Test
  def testFindNextSegmentMetadata(): Unit = {
    cache.assign(0, 0)
    cache.assign(1, 30)
    cache.assign(2, 100)

    val log: UnifiedLog = mock(classOf[UnifiedLog])
    when(log.leaderEpochCache).thenReturn(Option(cache))
    when(log.remoteLogEnabled()).thenReturn(true)

    val nextSegmentLeaderEpochs = new util.HashMap[Integer, lang.Long]
    nextSegmentLeaderEpochs.put(0, 0)
    nextSegmentLeaderEpochs.put(1, 30)
    nextSegmentLeaderEpochs.put(2, 100)
    val nextSegmentMetadata: RemoteLogSegmentMetadata =
      new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
        100, 199, -1L, brokerId, -1L, 1024, nextSegmentLeaderEpochs)
    val rlmmManager: RemoteLogMetadataManager = mock(classOf[RemoteLogMetadataManager])
    when(rlmmManager.remoteLogSegmentMetadata(ArgumentMatchers.eq(topicIdPartition), anyInt(), anyLong()))
      .thenAnswer((invocation: InvocationOnMock) => {
        val epoch = invocation.getArgument[Int](1)
        val nextOffset = invocation.getArgument[Long](2)
        if (epoch == 2 && nextOffset >= 100L && nextOffset <= 199L)
          Optional.of(nextSegmentMetadata)
        else
          Optional.empty()
      })
    when(rlmmManager.highestOffsetForEpoch(ArgumentMatchers.eq(topicIdPartition), anyInt()))
      .thenReturn(Optional.empty(): Optional[java.lang.Long])
    when(rlmmManager.listRemoteLogSegments(topicIdPartition)).thenReturn(Collections.emptyIterator(): java.util.Iterator[RemoteLogSegmentMetadata])

    val topic = topicIdPartition.topicPartition().topic()
    val partition: Partition = mock(classOf[Partition])
    when(partition.topic).thenReturn(topic)
    when(partition.topicPartition).thenReturn(topicIdPartition.topicPartition())
    when(partition.log).thenReturn(Option(log))
    when(partition.getLeaderEpoch).thenReturn(0)

    val remoteLogManager = new RemoteLogManager(_ => Option(log),
      (_, _) => {}, rlmConfig, time, 1, clusterId, logsDir, brokerTopicStats) {
      override private[remote] def createRemoteLogMetadataManager(): RemoteLogMetadataManager = rlmmManager
    }
    remoteLogManager.onLeadershipChange(Set(partition), Set(), Collections.singletonMap(topic, topicIdPartition.topicId()))

    val segmentLeaderEpochs = new util.HashMap[Integer, lang.Long]()
    segmentLeaderEpochs.put(0, 0)
    segmentLeaderEpochs.put(1, 30)
    // end offset is set to 99, the next offset to search in remote storage is 100
    var segmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
      0, 99, -1L, brokerId, -1L, 1024, segmentLeaderEpochs)
    assertEquals(Option(nextSegmentMetadata), remoteLogManager.findNextSegmentMetadata(segmentMetadata))

    // end offset is set to 105, the next offset to search in remote storage is 106
    segmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
      0, 105, -1L, brokerId, -1L, 1024, segmentLeaderEpochs)
    assertEquals(Option(nextSegmentMetadata), remoteLogManager.findNextSegmentMetadata(segmentMetadata))

    segmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
      0, 200, -1L, brokerId, -1L, 1024, segmentLeaderEpochs)
    assertEquals(None, remoteLogManager.findNextSegmentMetadata(segmentMetadata))
  }

  @Test
  def testAddAbortedTransactions(): Unit = {
    val baseOffset = 45
    val timeIdx = new TimeIndex(nonExistentTempFile(), baseOffset, maxIndexSize = 30 * 12)
    val txnIdx = new TransactionIndex(baseOffset, TestUtils.tempFile())
    val offsetIdx = new OffsetIndex(nonExistentTempFile(), baseOffset, maxIndexSize = 4 * 8)
    offsetIdx.append(baseOffset + 0, 0)
    offsetIdx.append(baseOffset + 1, 100)
    offsetIdx.append(baseOffset + 2, 200)
    offsetIdx.append(baseOffset + 3, 300)

    val rsmManager: ClassLoaderAwareRemoteStorageManager = mock(classOf[ClassLoaderAwareRemoteStorageManager])
    when(rsmManager.fetchIndex(any(), ArgumentMatchers.eq(IndexType.OFFSET))).thenReturn(new FileInputStream(offsetIdx.file))
    when(rsmManager.fetchIndex(any(), ArgumentMatchers.eq(IndexType.TIMESTAMP))).thenReturn(new FileInputStream(timeIdx.file))
    when(rsmManager.fetchIndex(any(), ArgumentMatchers.eq(IndexType.TRANSACTION))).thenReturn(new FileInputStream(txnIdx.file))

    val records: Records = mock(classOf[Records])
    when(records.sizeInBytes()).thenReturn(150)
    val fetchDataInfo = FetchDataInfo(LogOffsetMetadata(baseOffset, UnifiedLog.UnknownOffset, 0), records)

    var upperBoundOffsetCapture: Option[Long] = None

    val remoteLogManager =
      new RemoteLogManager(_ => None, (_, _) => {}, rlmConfig, time, 1, clusterId, logsDir, brokerTopicStats) {
        override private[remote] def createRemoteStorageManager(): ClassLoaderAwareRemoteStorageManager = rsmManager
        override private[remote] def collectAbortedTransactions(startOffset: Long,
                                                                upperBoundOffset: Long,
                                                                segmentMetadata: RemoteLogSegmentMetadata,
                                                                accumulator: List[AbortedTxn] => Unit): Unit = {
          upperBoundOffsetCapture = Option(upperBoundOffset)
        }
      }

    val segmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
      45, 99, -1L, brokerId, -1L, 1024, Collections.singletonMap(0, 45))

    // If base-offset=45 and fetch-size=150, then the upperBoundOffset=47
    val actualFetchDataInfo = remoteLogManager.addAbortedTransactions(baseOffset, segmentMetadata, fetchDataInfo)
    assertTrue(actualFetchDataInfo.abortedTransactions.isDefined)
    assertTrue(actualFetchDataInfo.abortedTransactions.get.isEmpty)
    assertEquals(Option(47), upperBoundOffsetCapture)

    // If base-offset=45 and fetch-size=301, then the entry won't exists in the offset index, returns next
    // remote/local segment base offset.
    upperBoundOffsetCapture = None
    reset(records)
    when(records.sizeInBytes()).thenReturn(301)
    remoteLogManager.addAbortedTransactions(baseOffset, segmentMetadata, fetchDataInfo)
    assertEquals(Option(100), upperBoundOffsetCapture)
  }

  @Test
  def testCollectAbortedTransactionsIteratesNextRemoteSegment(): Unit = {
    cache.assign(0, 0)

    val log: UnifiedLog = mock(classOf[UnifiedLog])
    when(log.leaderEpochCache).thenReturn(Option(cache))
    when(log.logSegments).thenReturn(Iterable.empty)
    when(log.remoteLogEnabled()).thenReturn(true)

    val baseOffset = 45
    val timeIdx = new TimeIndex(nonExistentTempFile(), baseOffset, maxIndexSize = 30 * 12)
    val txnIdx = new TransactionIndex(baseOffset, TestUtils.tempFile())
    val offsetIdx = new OffsetIndex(nonExistentTempFile(), baseOffset, maxIndexSize = 4 * 8)
    offsetIdx.append(baseOffset + 0, 0)
    offsetIdx.append(baseOffset + 1, 100)
    offsetIdx.append(baseOffset + 2, 200)
    offsetIdx.append(baseOffset + 3, 300)

    val nextTxnIdx = new TransactionIndex(100L, TestUtils.tempFile())
    val abortedTxns = List(
      new AbortedTxn(producerId = 0L, firstOffset = 50, lastOffset = 105, lastStableOffset = 60),
      new AbortedTxn(producerId = 1L, firstOffset = 55, lastOffset = 120, lastStableOffset = 100)
    )
    abortedTxns.foreach(nextTxnIdx.append)

    val nextSegmentMetadata: RemoteLogSegmentMetadata =
      new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
        100, 199, -1L, brokerId, -1L, 1024, Collections.singletonMap(0, 100))
    val rlmmManager: RemoteLogMetadataManager = mock(classOf[RemoteLogMetadataManager])
    when(rlmmManager.remoteLogSegmentMetadata(ArgumentMatchers.eq(topicIdPartition), anyInt(), anyLong())).thenAnswer((invocation: InvocationOnMock) => {
        val epoch = invocation.getArgument[Int](1)
        val nextOffset = invocation.getArgument[Long](2)
        if (epoch == 0 && nextOffset >= 100L && nextOffset <= 199L)
          Optional.of(nextSegmentMetadata)
        else
          Optional.empty()
      }
    )
    when(rlmmManager.highestOffsetForEpoch(ArgumentMatchers.eq(topicIdPartition), anyInt()))
      .thenReturn(Optional.empty(): Optional[java.lang.Long])
    when(rlmmManager.listRemoteLogSegments(topicIdPartition)).thenReturn(Collections.emptyIterator(): java.util.Iterator[RemoteLogSegmentMetadata])

    val rsmManager: ClassLoaderAwareRemoteStorageManager = mock(classOf[ClassLoaderAwareRemoteStorageManager])
    when(rsmManager.fetchIndex(any(), ArgumentMatchers.eq(IndexType.OFFSET))).thenReturn(new FileInputStream(offsetIdx.file))
    when(rsmManager.fetchIndex(any(), ArgumentMatchers.eq(IndexType.TIMESTAMP))).thenReturn(new FileInputStream(timeIdx.file))
    when(rsmManager.fetchIndex(any(), ArgumentMatchers.eq(IndexType.TRANSACTION))).thenAnswer((invocation: InvocationOnMock) => {
      val segmentMetadata = invocation.getArgument[RemoteLogSegmentMetadata](0)
      if (segmentMetadata.equals(nextSegmentMetadata)) {
        new FileInputStream(nextTxnIdx.file)
      } else {
        new FileInputStream(txnIdx.file)
      }
    })

    val records: Records = mock(classOf[Records])
    when(records.sizeInBytes()).thenReturn(301)
    val fetchDataInfo = FetchDataInfo(LogOffsetMetadata(baseOffset, UnifiedLog.UnknownOffset, 0), records)

    val topic = topicIdPartition.topicPartition().topic()
    val partition: Partition = mock(classOf[Partition])
    when(partition.topic).thenReturn(topic)
    when(partition.topicPartition).thenReturn(topicIdPartition.topicPartition())
    when(partition.log).thenReturn(Option(log))
    when(partition.getLeaderEpoch).thenReturn(0)

    val remoteLogManager =
      new RemoteLogManager(_ => Option(log), (_, _) => {}, rlmConfig, time, 1, clusterId, logsDir, brokerTopicStats) {
        override private[remote] def createRemoteLogMetadataManager(): RemoteLogMetadataManager = rlmmManager
        override private[remote] def createRemoteStorageManager(): ClassLoaderAwareRemoteStorageManager = rsmManager
      }
    remoteLogManager.onLeadershipChange(Set(partition), Set(), Collections.singletonMap(topic, topicIdPartition.topicId()))

    // If base-offset=45 and fetch-size=301, then the entry won't exists in the offset index, returns next
    // remote/local segment base offset.
    val segmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
      45, 99, -1L, brokerId, -1L, 1024, Collections.singletonMap(0, 45))
    val expectedFetchDataInfo = remoteLogManager.addAbortedTransactions(baseOffset, segmentMetadata, fetchDataInfo)

    assertTrue(expectedFetchDataInfo.abortedTransactions.isDefined)
    assertEquals(abortedTxns.map(_.asAbortedTransaction), expectedFetchDataInfo.abortedTransactions.get)
  }

  @Test
  def testCollectAbortedTransactionsIteratesNextLocalSegment(): Unit = {
    cache.assign(0, 0)

    val baseOffset = 45
    val timeIdx = new TimeIndex(nonExistentTempFile(), baseOffset, maxIndexSize = 30 * 12)
    val txnIdx = new TransactionIndex(baseOffset, TestUtils.tempFile())
    val offsetIdx = new OffsetIndex(nonExistentTempFile(), baseOffset, maxIndexSize = 4 * 8)
    offsetIdx.append(baseOffset + 0, 0)
    offsetIdx.append(baseOffset + 1, 100)
    offsetIdx.append(baseOffset + 2, 200)
    offsetIdx.append(baseOffset + 3, 300)

    val nextTxnIdx = new TransactionIndex(100L, TestUtils.tempFile())
    val abortedTxns = List(
      new AbortedTxn(producerId = 0L, firstOffset = 50, lastOffset = 105, lastStableOffset = 60),
      new AbortedTxn(producerId = 1L, firstOffset = 55, lastOffset = 120, lastStableOffset = 100)
    )
    abortedTxns.foreach(nextTxnIdx.append)

    val logSegment: LogSegment = mock(classOf[LogSegment])
    when(logSegment.txnIndex).thenReturn(nextTxnIdx)

    val log: UnifiedLog = mock(classOf[UnifiedLog])
    when(log.leaderEpochCache).thenReturn(Option(cache))
    when(log.logSegments).thenReturn(List(logSegment))
    when(log.remoteLogEnabled()).thenReturn(true)

    val rlmmManager: RemoteLogMetadataManager = mock(classOf[RemoteLogMetadataManager])
    when(rlmmManager.remoteLogSegmentMetadata(ArgumentMatchers.eq(topicIdPartition), anyInt(), anyLong()))
      .thenReturn(Optional.empty(): Optional[RemoteLogSegmentMetadata])
    when(rlmmManager.highestOffsetForEpoch(ArgumentMatchers.eq(topicIdPartition), anyInt()))
      .thenReturn(Optional.empty(): Optional[java.lang.Long])
    when(rlmmManager.listRemoteLogSegments(topicIdPartition)).thenReturn(Collections.emptyIterator(): java.util.Iterator[RemoteLogSegmentMetadata])

    val rsmManager: ClassLoaderAwareRemoteStorageManager = mock(classOf[ClassLoaderAwareRemoteStorageManager])
    when(rsmManager.fetchIndex(any(), ArgumentMatchers.eq(IndexType.OFFSET))).thenReturn(new FileInputStream(offsetIdx.file))
    when(rsmManager.fetchIndex(any(), ArgumentMatchers.eq(IndexType.TIMESTAMP))).thenReturn(new FileInputStream(timeIdx.file))
    when(rsmManager.fetchIndex(any(), ArgumentMatchers.eq(IndexType.TRANSACTION))).thenReturn(new FileInputStream(txnIdx.file))

    val records: Records = mock(classOf[Records])
    when(records.sizeInBytes()).thenReturn(301)
    val fetchDataInfo = FetchDataInfo(LogOffsetMetadata(baseOffset, UnifiedLog.UnknownOffset, 0), records)

    val topic = topicIdPartition.topicPartition().topic()
    val partition: Partition = mock(classOf[Partition])
    when(partition.topic).thenReturn(topic)
    when(partition.topicPartition).thenReturn(topicIdPartition.topicPartition())
    when(partition.log).thenReturn(Option(log))
    when(partition.getLeaderEpoch).thenReturn(0)

    val remoteLogManager =
      new RemoteLogManager(_ => Option(log), (_, _) => {}, rlmConfig, time, 1, clusterId, logsDir, brokerTopicStats) {
        override private[remote] def createRemoteLogMetadataManager(): RemoteLogMetadataManager = rlmmManager
        override private[remote] def createRemoteStorageManager(): ClassLoaderAwareRemoteStorageManager = rsmManager
      }
    remoteLogManager.onLeadershipChange(Set(partition), Set(), Collections.singletonMap(topic, topicIdPartition.topicId()))

    // If base-offset=45 and fetch-size=301, then the entry won't exists in the offset index, returns next
    // remote/local segment base offset.
    val segmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
      45, 99, -1L, brokerId, -1L, 1024, Collections.singletonMap(0, 45))
    val expectedFetchDataInfo = remoteLogManager.addAbortedTransactions(baseOffset, segmentMetadata, fetchDataInfo)

    assertTrue(expectedFetchDataInfo.abortedTransactions.isDefined)
    assertEquals(abortedTxns.map(_.asAbortedTransaction), expectedFetchDataInfo.abortedTransactions.get)
  }

  @Test
  def testGetClassLoaderAwareRemoteStorageManager(): Unit = {
    val rsmManager: ClassLoaderAwareRemoteStorageManager = mock(classOf[ClassLoaderAwareRemoteStorageManager])
    val remoteLogManager =
      new RemoteLogManager(_ => None, (_, _) => {}, rlmConfig, time, 1, clusterId, logsDir, brokerTopicStats) {
        override private[remote] def createRemoteStorageManager(): ClassLoaderAwareRemoteStorageManager = rsmManager
      }
    assertEquals(rsmManager, remoteLogManager.storageManager())
  }

  private def nonExistentTempFile(): File = {
    val file = TestUtils.tempFile()
    file.delete()
    file
  }

  private def createRLMConfig(props: Properties = new Properties): RemoteLogManagerConfig = {
    props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, true.toString)
    props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, "org.apache.kafka.server.log.remote.storage.NoOpRemoteStorageManager")
    props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, "org.apache.kafka.server.log.remote.storage.NoOpRemoteLogMetadataManager")
    val config = new AbstractConfig(RemoteLogManagerConfig.CONFIG_DEF, props)
    new RemoteLogManagerConfig(config)
  }
}