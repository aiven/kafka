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
import kafka.server.checkpoints.{LeaderEpochCheckpoint, LeaderEpochCheckpointFile}
import kafka.server.epoch.{EpochEntry, LeaderEpochFileCache}
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType
import org.apache.kafka.server.log.remote.storage._
import org.mockito.Mockito.{doNothing, mock, reset, when}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.{AfterEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.ArgumentMatchers.{any, anyInt, anyLong}
import org.mockito.invocation.InvocationOnMock

import java.io.{File, FileInputStream}
import java.nio.file.{Files, Paths}
import java.util.concurrent.CompletableFuture
import java.util.{Collections, Optional, Properties}
import java.{lang, util}
import scala.collection.{Seq, mutable}
import scala.jdk.CollectionConverters._

class RemoteLogManagerTest {

  val clusterId = "test-cluster-id"
  val brokerId = 0
  val topicPartition = new TopicPartition("test-topic", 0)
  val topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), topicPartition)
  val time = new MockTime()
  val brokerTopicStats = new BrokerTopicStats
  val logsDir: String = Files.createTempDirectory("kafka-").toString
  val cache = new LeaderEpochFileCache(topicPartition, checkpoint())
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

  @ParameterizedTest(name = "testDeleteLogSegmentDueToRetentionTimeBreach segmentCount={0} deletableSegmentCount={1}")
  @CsvSource(value = Array("50, 0", "50, 1", "50, 23", "50, 50"))
  def testDeleteLogSegmentDueToRetentionTimeBreach(segmentCount: Int, deletableSegmentCount: Int): Unit = {
    val recordsPerSegment = 100
    val epochCheckpoints = Seq(0 -> 0, 1 -> 20, 3 -> 50, 4 -> 100)
    epochCheckpoints.foreach { case (epoch, startOffset) => cache.assign(epoch, startOffset) }
    val currentLeaderEpoch = epochCheckpoints.last._1

    val logConfig: LogConfig = mock(classOf[LogConfig])
    when(logConfig.retentionMs).thenReturn(1)
    when(logConfig.retentionSize).thenReturn(-1)

    val log: UnifiedLog = mock(classOf[UnifiedLog])
    when(log.leaderEpochCache).thenReturn(Option(cache))
    when(log.config).thenReturn(logConfig)
    when(log.size).thenReturn(0)

    var logStartOffset: Option[Long] = None
    val rsmManager: ClassLoaderAwareRemoteStorageManager = mock(classOf[ClassLoaderAwareRemoteStorageManager])
    val rlmmManager: RemoteLogMetadataManager = mock(classOf[RemoteLogMetadataManager])
    val remoteLogManager =
      new RemoteLogManager(_ => Option(log), (_, startOffset) => logStartOffset = Option(startOffset), rlmConfig, time,
        brokerId, clusterId, logsDir, brokerTopicStats) {
        override private[remote] def createRemoteStorageManager(): ClassLoaderAwareRemoteStorageManager = rsmManager
        override private[remote] def createRemoteLogMetadataManager() = rlmmManager
      }
    val segmentMetadataList = listRemoteLogSegmentMetadataByTime(segmentCount, deletableSegmentCount, recordsPerSegment)
    when(rlmmManager.highestOffsetForEpoch(ArgumentMatchers.eq(topicIdPartition), anyInt()))
      .thenReturn(Optional.empty(): Optional[java.lang.Long])
    when(rlmmManager.listRemoteLogSegments(topicIdPartition)).thenReturn(segmentMetadataList.iterator.asJava)
    when(rlmmManager.listRemoteLogSegments(ArgumentMatchers.eq(topicIdPartition), anyInt())).thenAnswer((invocation: InvocationOnMock) => {
      val leaderEpoch = invocation.getArgument[Int](1)
      if (leaderEpoch == 0)
        segmentMetadataList.take(1).iterator.asJava
      else if (leaderEpoch == 4)
        segmentMetadataList.drop(1).iterator.asJava
      else
        Collections.emptyIterator()
    })
    when(rlmmManager.updateRemoteLogSegmentMetadata(any(classOf[RemoteLogSegmentMetadataUpdate])))
      .thenReturn(CompletableFuture.completedFuture(null): CompletableFuture[Void])

    val args1: ArgumentCaptor[RemoteLogSegmentMetadata] = ArgumentCaptor.forClass(classOf[RemoteLogSegmentMetadata])
    doNothing().when(rsmManager).deleteLogSegmentData(args1.capture())

    val rlmTask = new remoteLogManager.RLMTask(topicIdPartition)
    rlmTask.convertToLeader(currentLeaderEpoch)
    rlmTask.handleExpiredRemoteLogSegments()

    assertEquals(deletableSegmentCount, args1.getAllValues.size())
    if (deletableSegmentCount > 0) {
      val expectedEndMetadata = segmentMetadataList(deletableSegmentCount-1)
      assertEquals(segmentMetadataList.head, args1.getAllValues.asScala.head)
      assertEquals(expectedEndMetadata, args1.getAllValues.asScala.reverse.head)
      assertEquals(expectedEndMetadata.endOffset()+1, logStartOffset.get)
    }
  }

  @ParameterizedTest(name = "testDeleteLogSegmentDueToRetentionSizeBreach segmentCount={0} deletableSegmentCount={1}")
  @CsvSource(value = Array("50, 0", "50, 1", "50, 23", "50, 50"))
  def testDeleteLogSegmentDueToRetentionSizeBreach(segmentCount: Int, deletableSegmentCount: Int): Unit = {
    val recordsPerSegment = 100
    val epochCheckpoints = Seq(0 -> 0, 1 -> 20, 3 -> 50, 4 -> 100)
    epochCheckpoints.foreach { case (epoch, startOffset) => cache.assign(epoch, startOffset) }
    val currentLeaderEpoch = epochCheckpoints.last._1

    val localLogSegmentsSize = 500L
    val retentionSize = (segmentCount - deletableSegmentCount) * 100 + localLogSegmentsSize
    val logConfig: LogConfig = mock(classOf[LogConfig])
    when(logConfig.retentionMs).thenReturn(-1)
    when(logConfig.retentionSize).thenReturn(retentionSize)

    val log: UnifiedLog = mock(classOf[UnifiedLog])
    when(log.leaderEpochCache).thenReturn(Option(cache))
    when(log.config).thenReturn(logConfig)
    when(log.size).thenReturn(localLogSegmentsSize)

    var logStartOffset: Option[Long] = None
    val rsmManager: ClassLoaderAwareRemoteStorageManager = mock(classOf[ClassLoaderAwareRemoteStorageManager])
    val rlmmManager: RemoteLogMetadataManager = mock(classOf[RemoteLogMetadataManager])
    val remoteLogManager =
      new RemoteLogManager(_ => Option(log), (_, startOffset) => logStartOffset = Option(startOffset), rlmConfig, time,
        brokerId, clusterId, logsDir, brokerTopicStats) {
        override private[remote] def createRemoteStorageManager(): ClassLoaderAwareRemoteStorageManager = rsmManager
        override private[remote] def createRemoteLogMetadataManager() = rlmmManager
      }
    val segmentMetadataList = listRemoteLogSegmentMetadata(segmentCount, recordsPerSegment)
    when(rlmmManager.highestOffsetForEpoch(ArgumentMatchers.eq(topicIdPartition), anyInt()))
      .thenReturn(Optional.empty(): Optional[java.lang.Long])
    when(rlmmManager.listRemoteLogSegments(topicIdPartition)).thenReturn(segmentMetadataList.iterator.asJava)
    when(rlmmManager.listRemoteLogSegments(ArgumentMatchers.eq(topicIdPartition), anyInt())).thenAnswer((invocation: InvocationOnMock) => {
      val leaderEpoch = invocation.getArgument[Int](1)
      if (leaderEpoch == 0)
        segmentMetadataList.take(1).iterator.asJava
      else if (leaderEpoch == 4)
        segmentMetadataList.drop(1).iterator.asJava
      else
        Collections.emptyIterator()
    })
    when(rlmmManager.updateRemoteLogSegmentMetadata(any(classOf[RemoteLogSegmentMetadataUpdate])))
      .thenReturn(CompletableFuture.completedFuture(null): CompletableFuture[Void])

    val args1: ArgumentCaptor[RemoteLogSegmentMetadata] = ArgumentCaptor.forClass(classOf[RemoteLogSegmentMetadata])
    doNothing().when(rsmManager).deleteLogSegmentData(args1.capture())

    val rlmTask = new remoteLogManager.RLMTask(topicIdPartition)
    rlmTask.convertToLeader(currentLeaderEpoch)
    rlmTask.handleExpiredRemoteLogSegments()

    assertEquals(deletableSegmentCount, args1.getAllValues.size())
    if (deletableSegmentCount > 0) {
      val expectedEndMetadata = segmentMetadataList(deletableSegmentCount-1)
      assertEquals(segmentMetadataList.head, args1.getAllValues.asScala.head)
      assertEquals(expectedEndMetadata, args1.getAllValues.asScala.reverse.head)
      assertEquals(expectedEndMetadata.endOffset()+1, logStartOffset.get)
    }
  }

  @ParameterizedTest(name = "testDeleteLogSegmentDueToRetentionTimeAndSizeBreach segmentCount={0} deletableSegmentCountByTime={1} deletableSegmentCountBySize={2}")
  @CsvSource(value = Array("50, 0, 0", "50, 1, 5", "50, 23, 15", "50, 50, 50"))
  def testDeleteLogSegmentDueToRetentionTimeAndSizeBreach(segmentCount: Int,
                                                          deletableSegmentCountByTime: Int,
                                                          deletableSegmentCountBySize: Int): Unit = {
    val recordsPerSegment = 100
    val epochCheckpoints = Seq(0 -> 0, 1 -> 20, 3 -> 50, 4 -> 100)
    epochCheckpoints.foreach { case (epoch, startOffset) => cache.assign(epoch, startOffset) }
    val currentLeaderEpoch = epochCheckpoints.last._1

    val localLogSegmentsSize = 500L
    val retentionSize = (segmentCount - deletableSegmentCountBySize) * 100 + localLogSegmentsSize
    val logConfig: LogConfig = mock(classOf[LogConfig])
    when(logConfig.retentionMs).thenReturn(1)
    when(logConfig.retentionSize).thenReturn(retentionSize)

    val log: UnifiedLog = mock(classOf[UnifiedLog])
    when(log.leaderEpochCache).thenReturn(Option(cache))
    when(log.config).thenReturn(logConfig)
    when(log.size).thenReturn(localLogSegmentsSize)

    var logStartOffset: Option[Long] = None
    val rsmManager: ClassLoaderAwareRemoteStorageManager = mock(classOf[ClassLoaderAwareRemoteStorageManager])
    val rlmmManager: RemoteLogMetadataManager = mock(classOf[RemoteLogMetadataManager])
    val remoteLogManager =
      new RemoteLogManager(_ => Option(log), (_, startOffset) => logStartOffset = Option(startOffset), rlmConfig, time,
        brokerId, clusterId, logsDir, brokerTopicStats) {
        override private[remote] def createRemoteStorageManager(): ClassLoaderAwareRemoteStorageManager = rsmManager
        override private[remote] def createRemoteLogMetadataManager() = rlmmManager
      }
    val segmentMetadataList = listRemoteLogSegmentMetadataByTime(segmentCount, deletableSegmentCountByTime, recordsPerSegment)
    when(rlmmManager.highestOffsetForEpoch(ArgumentMatchers.eq(topicIdPartition), anyInt()))
      .thenReturn(Optional.empty(): Optional[java.lang.Long])
    when(rlmmManager.listRemoteLogSegments(topicIdPartition)).thenReturn(segmentMetadataList.iterator.asJava)
    when(rlmmManager.listRemoteLogSegments(ArgumentMatchers.eq(topicIdPartition), anyInt())).thenAnswer((invocation: InvocationOnMock) => {
      val leaderEpoch = invocation.getArgument[Int](1)
      if (leaderEpoch == 0)
        segmentMetadataList.take(1).iterator.asJava
      else if (leaderEpoch == 4)
        segmentMetadataList.drop(1).iterator.asJava
      else
        Collections.emptyIterator()
    })
    when(rlmmManager.updateRemoteLogSegmentMetadata(any(classOf[RemoteLogSegmentMetadataUpdate])))
      .thenReturn(CompletableFuture.completedFuture(null): CompletableFuture[Void])

    val args1: ArgumentCaptor[RemoteLogSegmentMetadata] = ArgumentCaptor.forClass(classOf[RemoteLogSegmentMetadata])
    doNothing().when(rsmManager).deleteLogSegmentData(args1.capture())

    val rlmTask = new remoteLogManager.RLMTask(topicIdPartition)
    rlmTask.convertToLeader(currentLeaderEpoch)
    rlmTask.handleExpiredRemoteLogSegments()

    val deletableSegmentCount = Math.max(deletableSegmentCountBySize, deletableSegmentCountByTime)
    assertEquals(deletableSegmentCount, args1.getAllValues.size())
    if (deletableSegmentCount > 0) {
      val expectedEndMetadata = segmentMetadataList(deletableSegmentCount-1)
      assertEquals(segmentMetadataList.head, args1.getAllValues.asScala.head)
      assertEquals(expectedEndMetadata, args1.getAllValues.asScala.reverse.head)
      assertEquals(expectedEndMetadata.endOffset()+1, logStartOffset.get)
    }
  }

  @Test
  def testGetLeaderEpochCheckpoint(): Unit = {
    val epochs = Seq(EpochEntry(0, 33), EpochEntry(1, 43), EpochEntry(2, 99), EpochEntry(3, 105))
    epochs.foreach(epochEntry => cache.assign(epochEntry.epoch, epochEntry.startOffset))

    val log: UnifiedLog = mock(classOf[UnifiedLog])
    when(log.leaderEpochCache).thenReturn(Option(cache))
    val remoteLogManager = new RemoteLogManager(_ => Option(log), (_, _) => {}, rlmConfig, time, brokerId = 1,
      clusterId, logsDir, brokerTopicStats)

    var actual = remoteLogManager.getLeaderEpochCheckpoint(log, startOffset = -1, endOffset = 200).read()
    assertEquals(epochs.take(4), actual)
    actual = remoteLogManager.getLeaderEpochCheckpoint(log, startOffset = -1, endOffset = 105).read()
    assertEquals(epochs.take(3), actual)
    actual = remoteLogManager.getLeaderEpochCheckpoint(log, startOffset = -1, endOffset = 100).read()
    assertEquals(epochs.take(3), actual)

    actual = remoteLogManager.getLeaderEpochCheckpoint(log, startOffset = 34, endOffset = 200).read()
    assertEquals(Seq(EpochEntry(0, 34)) ++ epochs.slice(1, 4), actual)
    actual = remoteLogManager.getLeaderEpochCheckpoint(log, startOffset = 43, endOffset = 200).read()
    assertEquals(epochs.slice(1, 4), actual)

    actual = remoteLogManager.getLeaderEpochCheckpoint(log, startOffset = 34, endOffset = 100).read()
    assertEquals(Seq(EpochEntry(0, 34)) ++ epochs.slice(1, 3), actual)
    actual = remoteLogManager.getLeaderEpochCheckpoint(log, startOffset = 34, endOffset = 30).read()
    assertTrue(actual.isEmpty)
    actual = remoteLogManager.getLeaderEpochCheckpoint(log, startOffset = 101, endOffset = 101).read()
    assertTrue(actual.isEmpty)
    actual = remoteLogManager.getLeaderEpochCheckpoint(log, startOffset = 101, endOffset = 102).read()
    assertEquals(Seq(EpochEntry(2, 101)), actual)
  }

  @Test
  def testGetLeaderEpochCheckpointEncoding(): Unit = {
    val epochs = Seq(EpochEntry(0, 33), EpochEntry(1, 43), EpochEntry(2, 99), EpochEntry(3, 105))
    epochs.foreach(epochEntry => cache.assign(epochEntry.epoch, epochEntry.startOffset))

    val log: UnifiedLog = mock(classOf[UnifiedLog])
    when(log.leaderEpochCache).thenReturn(Option(cache))
    val remoteLogManager = new RemoteLogManager(_ => Option(log), (_, _) => {}, rlmConfig, time, brokerId = 1,
      clusterId, logsDir, brokerTopicStats)

    val tmpFile = TestUtils.tempFile()
    val checkpoint = remoteLogManager.getLeaderEpochCheckpoint(log, startOffset = -1, endOffset = 200)
    assertEquals(epochs, checkpoint.read())

    Files.write(tmpFile.toPath, checkpoint.readAsByteBuffer().array())
    assertEquals(epochs, new LeaderEpochCheckpointFile(tmpFile).read())
  }

  private def listRemoteLogSegmentMetadataByTime(segmentCount: Int,
                                                 deletableSegmentCount: Int,
                                                 recordsPerSegment: Int): List[RemoteLogSegmentMetadata] = {
    val result = mutable.Buffer.empty[RemoteLogSegmentMetadata]
    for (idx <- 0 until segmentCount) {
      val timestamp = if (idx < deletableSegmentCount) time.milliseconds()-1 else time.milliseconds()
      val startOffset = idx * recordsPerSegment
      val endOffset = startOffset + recordsPerSegment - 1
      val segmentLeaderEpochs = truncateAndGetLeaderEpochs(cache, startOffset, endOffset)
      result += new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
        startOffset, endOffset, timestamp, brokerId, timestamp, 100, segmentLeaderEpochs)
    }
    result.toList
  }

  private def listRemoteLogSegmentMetadata(segmentCount: Int, recordsPerSegment: Int): List[RemoteLogSegmentMetadata] = {
    listRemoteLogSegmentMetadataByTime(segmentCount, 0, recordsPerSegment)
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

  private def checkpoint(): LeaderEpochCheckpoint = {
    new LeaderEpochCheckpoint {
      private var epochs: Seq[EpochEntry] = Seq()
      override def write(epochs: Iterable[EpochEntry]): Unit = this.epochs = epochs.toSeq
      override def read(): Seq[EpochEntry] = this.epochs
    }
  }

  private def truncateAndGetLeaderEpochs(cache: LeaderEpochFileCache,
                                         startOffset: Long,
                                         endOffset: Long): util.Map[Integer, lang.Long] = {
    val myCheckpoint = checkpoint()
    val myCache = cache.writeTo(myCheckpoint)
    myCache.truncateFromStart(startOffset)
    myCache.truncateFromEnd(endOffset)
    myCheckpoint.read().map(e => Integer.valueOf(e.epoch) -> lang.Long.valueOf(e.startOffset)).toMap.asJava
  }
}