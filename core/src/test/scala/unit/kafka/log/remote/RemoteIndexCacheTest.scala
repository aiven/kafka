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

import java.io.{File, FileInputStream}
import java.nio.file.Files
import java.util.Collections
import kafka.log.{OffsetIndex, OffsetPosition, TimeIndex}
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType
import org.apache.kafka.server.log.remote.storage.{RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteStorageManager}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.mockito.ArgumentMatchers.any
import org.mockito.{ArgumentMatchers, Mockito}
import org.mockito.Mockito.{reset, when}

class RemoteIndexCacheTest {

  val rlsm: RemoteStorageManager = Mockito.mock(classOf[RemoteStorageManager])
  var rlsMetadata: RemoteLogSegmentMetadata = _
  var cache: RemoteIndexCache = _
  var offsetIndex: OffsetIndex = _
  var timeIndex: TimeIndex = _
  val maxEntries = 30
  val baseOffset = 45L

  @BeforeEach
  def setup(): Unit = {
    offsetIndex = new OffsetIndex(createTempFile(), baseOffset, maxIndexSize = maxEntries * 8)
    timeIndex = new TimeIndex(createTempFile(), baseOffset = baseOffset, maxIndexSize = maxEntries * 12)

    appendIndexEntries()

    // fetch indexes only once to build the cache, later it should be available in the cache
    when(rlsm.fetchIndex(any(classOf[RemoteLogSegmentMetadata]), ArgumentMatchers.eq(IndexType.OFFSET)))
      .thenReturn(new FileInputStream(offsetIndex.file))
    when(rlsm.fetchIndex(any(classOf[RemoteLogSegmentMetadata]), ArgumentMatchers.eq(IndexType.TIMESTAMP)))
      .thenReturn(new FileInputStream(timeIndex.file))
    when(rlsm.fetchIndex(any(classOf[RemoteLogSegmentMetadata]), ArgumentMatchers.eq(IndexType.TRANSACTION)))
      .thenReturn(new FileInputStream(File.createTempFile("kafka-test-", ".txnIndex")))
    when(rlsm.fetchIndex(any(classOf[RemoteLogSegmentMetadata]), ArgumentMatchers.eq(IndexType.PRODUCER_SNAPSHOT)))
      .thenReturn(new FileInputStream(File.createTempFile("kafka-test-", ".snapshot")))

    val logDir = Files.createTempDirectory("kafka-").toString
    cache = new RemoteIndexCache(remoteStorageManager = rlsm, logDir = logDir)

    val topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0))
    rlsMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
      baseOffset, offsetIndex.lastOffset, -1L, 1, 1024, 1024,
      Collections.singletonMap(0, 0L))
  }

  private def appendIndexEntries(): Unit = {
    val curTime = System.currentTimeMillis()
    for (i <- 0 until offsetIndex.maxEntries) {
      val offset = offsetIndex.baseOffset + i + 1
      offsetIndex.append(offset, i)
      timeIndex.maybeAppend(curTime + i, offset, skipFullCheck = true)
    }

    offsetIndex.flush()
    timeIndex.flush()
  }

  private def assertIndexEntries(): Unit = {
    for (i <- 0 until offsetIndex.maxEntries)
      assertEquals(OffsetPosition(offsetIndex.baseOffset + i + 1, i), offsetIndex.entry(i))
    assertTrue(timeIndex.entries > 0)
    assertTrue(timeIndex.file.exists())
  }

  def createTempFile(): File = {
    val file = File.createTempFile("kafka-test-", ".tmp")
    Files.delete(file.toPath)
    file
  }

  @AfterEach
  def cleanup(): Unit = {
    reset(rlsm)

    if (offsetIndex != null) offsetIndex.deleteIfExists()
    if (timeIndex != null) timeIndex.deleteIfExists()
    cache.close()
  }

  @Test
  def testLoadingIndexFromRemoteStorage(): Unit = {

    assertIndexEntries()

    val offsetPosition1 = offsetIndex.entry(1)
    // this call should have invoked fetchOffsetIndex, fetchTimestampIndex once
    val resultOffset = cache.lookupOffset(rlsMetadata, offsetPosition1.offset)
    assertEquals(offsetPosition1.position, resultOffset)

    // this should not cause fetching index from RemoteLogStorageManager as it is already fetched earlier
    // this is checked by setting expectation times as 1 on the mock
    val offsetPosition2 = offsetIndex.entry(2)
    val resultOffset2 = cache.lookupOffset(rlsMetadata, offsetPosition2.offset)
    assertEquals(offsetPosition2.position, resultOffset2)
  }

  @Test
  def testPositionForNonExistingIndexFromRemoteStorage(): Unit = {
    // offsets beyond this
    val lastOffsetPosition = cache.lookupOffset(rlsMetadata, offsetIndex.lastOffset)
    val greaterOffsetThanLastOffset = offsetIndex.lastOffset + 1
    val resultOffsetPosition1 = cache.lookupOffset(rlsMetadata, greaterOffsetThanLastOffset)
    assertEquals(lastOffsetPosition, resultOffsetPosition1)

    val nonExistentOffsetPosition = OffsetPosition(baseOffset, 0)
    val lowerOffsetThanBaseOffset = offsetIndex.baseOffset - 1
    val resultOffsetPosition2 = cache.lookupOffset(rlsMetadata, lowerOffsetThanBaseOffset)
    assertEquals(nonExistentOffsetPosition.position, resultOffsetPosition2)
  }
}
