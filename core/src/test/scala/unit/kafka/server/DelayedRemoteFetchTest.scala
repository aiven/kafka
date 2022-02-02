/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import java.util.Optional
import java.util.concurrent.CompletableFuture
import kafka.cluster.Partition
import kafka.log.remote.{MockRemoteLogManager, RemoteLogManager, RemoteLogReadResult}
import kafka.server.QuotaFactory.UnboundedQuota
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.utils.Utils
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{AfterEach, Test}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}

import java.nio.file.{Files, Path}
import scala.collection.Seq
import scala.jdk.CollectionConverters._

class DelayedRemoteFetchTest {
  val tp = new TopicPartition("test", 0)
  val tp1 = new TopicPartition("t1", 0)
  val topicId = Uuid.randomUuid()
  var isRemoteFetchExecuted = false
  val logDir: Path = Files.createTempDirectory("kafka-test-")
  val rlm = new MockRemoteLogManager(5, 20, logDir.toString)

  @AfterEach
  def afterEach(): Unit = {
    Utils.delete(logDir.toFile)
  }

  @Test
  def testRemoteFetch(): Unit = {
    var replied = false

    def responseCallback(r: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
      replied = true
      assert(r.head._1.equals(new TopicIdPartition(topicId, tp1)))

      assert(r(1)._1.equals(new TopicIdPartition(topicId, tp)))
      assertEquals(None, r(1)._2.error)
      assertEquals(2, r(1)._2.records.records.asScala.size)
      assertEquals(102, r(1)._2.records.records.iterator.next.offset)
      assertEquals(2000, r(1)._2.highWatermark)
    }

    RemoteFetch(timeout = false, responseCallback)
    assert(replied)
    assert(isRemoteFetchExecuted)
  }

  @Test
  def testRemoteFetchTimeout(): Unit = {
    var replied = false

    def responseCallback(r: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
      replied = true
      assert(r.head._1.equals(new TopicIdPartition(topicId, tp1)))

      assert(r(1)._1.equals(new TopicIdPartition(topicId, tp)))
      assertEquals(None, r(1)._2.error)
      assertEquals(0, r(1)._2.records.records.asScala.size)
      assertEquals(2000, r(1)._2.highWatermark)
    }

    RemoteFetch(timeout = true, responseCallback)
    assert(replied)
    assert(!isRemoteFetchExecuted)
  }

  private def RemoteFetch(timeout: Boolean, responseCallback: Seq[(TopicIdPartition, FetchPartitionData)] => Unit): Unit = {
    val fetchInfo = new PartitionData(topicId, 100, 0, 1000, Optional.of(1))
    val remoteFetchInfo = RemoteStorageFetchInfo(1000, minOneMessage = true, tp, fetchInfo, FetchTxnCommitted)

    val fetchPartitionStatus = FetchPartitionStatus(new LogOffsetMetadata(fetchInfo.fetchOffset), fetchInfo)

    val fetchPartitionStatus1 = FetchPartitionStatus(new LogOffsetMetadata(messageOffset = 50L, segmentBaseOffset = 0L,
      relativePositionInSegment = 250), new PartitionData(topicId, 50, 0, 1, Optional.empty()))

    val fetchParams = FetchParams(
      requestVersion = ApiKeys.FETCH.latestVersion,
      replicaId = 1,
      maxWaitMs = 0,
      minBytes = 1,
      maxBytes = 1000,
      isolation = FetchLogEnd,
      clientMetadata = None
    )

    val localReadResults: Seq[(TopicIdPartition, LogReadResult)] = List(
      (new TopicIdPartition(topicId, tp1), LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
        divergingEpoch = None,
        highWatermark = -1L,
        leaderLogStartOffset = -1L,
        leaderLogEndOffset = -1L,
        followerLogStartOffset = -1L,
        fetchTimeMs = -1L,
        lastStableOffset = None,
        exception = Some(new Exception()))),
      (new TopicIdPartition(topicId, tp), LogReadResult(
        FetchDataInfo(LogOffsetMetadata(fetchInfo.fetchOffset), MemoryRecords.EMPTY, delayedRemoteStorageFetch = Some(remoteFetchInfo)),
        divergingEpoch = None,
        highWatermark = 2000,
        leaderLogStartOffset = 0,
        leaderLogEndOffset = 2000,
        followerLogStartOffset = 0,
        fetchTimeMs = 0,
        lastStableOffset = Some(1000),
        exception = None))
    )

    val partition: Partition = mock(classOf[Partition])
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
    val replicaQuota = mock(classOf[ReplicaQuota])
    when(replicaManager.getPartitionOrException(any[TopicPartition]))
      .thenReturn(partition)

    val remoteFetchPurgatory = DelayedOperationPurgatory[DelayedRemoteFetch](purgatoryName = "RemoteFetch", brokerId = 1, purgeInterval = 50)
    val key = new TopicPartitionOperationKey(tp.topic(), tp.partition())
    val remoteFetchResult = new CompletableFuture[RemoteLogReadResult]
    var remoteFetchTask: RemoteLogManager#AsyncReadTask = null


    val delayedFetch = new DelayedFetch(
      fetchParams,
      fetchPartitionStatus = List(
        (new TopicIdPartition(topicId, tp1), fetchPartitionStatus1),
        (new TopicIdPartition(topicId, tp), fetchPartitionStatus)
      ),
      replicaManager = replicaManager,
      quota = replicaQuota,
      responseCallback = responseCallback
    )

    val remoteFetch = new DelayedRemoteFetch(remoteFetchTask = remoteFetchTask,
      remoteFetchResult = remoteFetchResult,
      remoteFetchInfo = localReadResults(1)._2.info.delayedRemoteStorageFetch.get,
      delayMs = 500,
      delayedFetch = delayedFetch,
      localReadResults = localReadResults,
      replicaManager = replicaManager,
      UnboundedQuota)

    assertEquals(false, remoteFetchPurgatory.tryCompleteElseWatch(remoteFetch, Seq(key)))

    if (timeout)
      rlm.pause()

    remoteFetchTask = rlm.asyncRead(remoteFetchInfo, (result: RemoteLogReadResult) => {
      isRemoteFetchExecuted = true
      remoteFetchResult.complete(result)
      remoteFetchPurgatory.checkAndComplete(key)
    })

    Thread.sleep(100)

    if (timeout) {
      assertEquals(false, remoteFetch.isCompleted)
      Thread.sleep(500)
    }

    assertEquals(true, remoteFetch.isCompleted)
  }
}


