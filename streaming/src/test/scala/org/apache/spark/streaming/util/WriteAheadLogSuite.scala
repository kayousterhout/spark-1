/*
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
package org.apache.spark.streaming.util

import java.io._
import java.nio.ByteBuffer
import java.util.concurrent.{ExecutionException, ThreadPoolExecutor}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.{implicitConversions, postfixOps}
import scala.util.{Failure, Success}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.mockito.Matchers.{eq => meq}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.Eventually._
import org.scalatest.{BeforeAndAfterEach, BeforeAndAfter}
import org.scalatest.mock.MockitoSugar

import org.apache.spark.streaming.scheduler._
import org.apache.spark.util.{ThreadUtils, ManualClock, Utils}
import org.apache.spark.{SparkException, SparkConf, SparkFunSuite}

/** Common tests for WriteAheadLogs that we would like to test with different configurations. */
abstract class CommonWriteAheadLogTests(
    allowBatching: Boolean,
    closeFileAfterWrite: Boolean,
    testTag: String = "")
  extends SparkFunSuite with BeforeAndAfter {

  import WriteAheadLogSuite._

  protected val hadoopConf = new Configuration()
  protected var tempDir: File = null
  protected var testDir: String = null
  protected var testFile: String = null
  protected var writeAheadLog: WriteAheadLog = null
  protected def testPrefix = if (testTag != "") testTag + " - " else testTag

  before {
    tempDir = Utils.createTempDir()
    testDir = tempDir.toString
    testFile = new File(tempDir, "testFile").toString
    if (writeAheadLog != null) {
      writeAheadLog.close()
      writeAheadLog = null
    }
  }

  after {
    Utils.deleteRecursively(tempDir)
  }

  test(testPrefix + "read all logs") {
    // Write data manually for testing reading through WriteAheadLog
    val writtenData = (1 to 10).map { i =>
      val data = generateRandomData()
      val file = testDir + s"/log-$i-$i"
      writeDataManually(data, file, allowBatching)
      data
    }.flatten

    val logDirectoryPath = new Path(testDir)
    val fileSystem = HdfsUtils.getFileSystemForPath(logDirectoryPath, hadoopConf)
    assert(fileSystem.exists(logDirectoryPath) === true)

    // Read data using manager and verify
    val readData = readDataUsingWriteAheadLog(testDir, closeFileAfterWrite, allowBatching)
    assert(readData === writtenData)
  }

  test(testPrefix + "write logs") {
    // Write data with rotation using WriteAheadLog class
    val dataToWrite = generateRandomData()
    writeDataUsingWriteAheadLog(testDir, dataToWrite, closeFileAfterWrite = closeFileAfterWrite,
      allowBatching = allowBatching)

    // Read data manually to verify the written data
    val logFiles = getLogFilesInDirectory(testDir)
    assert(logFiles.size > 1)
    val writtenData = readAndDeserializeDataManually(logFiles, allowBatching)
    assert(writtenData === dataToWrite)
  }

  test(testPrefix + "read all logs after write") {
    // Write data with manager, recover with new manager and verify
    val dataToWrite = generateRandomData()
    writeDataUsingWriteAheadLog(testDir, dataToWrite, closeFileAfterWrite, allowBatching)
    val logFiles = getLogFilesInDirectory(testDir)
    assert(logFiles.size > 1)
    val readData = readDataUsingWriteAheadLog(testDir, closeFileAfterWrite, allowBatching)
    assert(dataToWrite === readData)
  }

  test(testPrefix + "clean old logs") {
    logCleanUpTest(waitForCompletion = false)
  }

  test(testPrefix + "clean old logs synchronously") {
    logCleanUpTest(waitForCompletion = true)
  }

  private def logCleanUpTest(waitForCompletion: Boolean): Unit = {
    // Write data with manager, recover with new manager and verify
    val manualClock = new ManualClock
    val dataToWrite = generateRandomData()
    writeAheadLog = writeDataUsingWriteAheadLog(testDir, dataToWrite, closeFileAfterWrite,
      allowBatching, manualClock, closeLog = false)
    val logFiles = getLogFilesInDirectory(testDir)
    assert(logFiles.size > 1)

    writeAheadLog.clean(manualClock.getTimeMillis() / 2, waitForCompletion)

    if (waitForCompletion) {
      assert(getLogFilesInDirectory(testDir).size < logFiles.size)
    } else {
      eventually(Eventually.timeout(1 second), interval(10 milliseconds)) {
        assert(getLogFilesInDirectory(testDir).size < logFiles.size)
      }
    }
  }

  test(testPrefix + "handling file errors while reading rotating logs") {
    // Generate a set of log files
    val manualClock = new ManualClock
    val dataToWrite1 = generateRandomData()
    writeDataUsingWriteAheadLog(testDir, dataToWrite1, closeFileAfterWrite, allowBatching,
      manualClock)
    val logFiles1 = getLogFilesInDirectory(testDir)
    assert(logFiles1.size > 1)


    // Recover old files and generate a second set of log files
    val dataToWrite2 = generateRandomData()
    manualClock.advance(100000)
    writeDataUsingWriteAheadLog(testDir, dataToWrite2, closeFileAfterWrite, allowBatching ,
      manualClock)
    val logFiles2 = getLogFilesInDirectory(testDir)
    assert(logFiles2.size > logFiles1.size)

    // Read the files and verify that all the written data can be read
    val readData1 = readDataUsingWriteAheadLog(testDir, closeFileAfterWrite, allowBatching)
    assert(readData1 === (dataToWrite1 ++ dataToWrite2))

    // Corrupt the first set of files so that they are basically unreadable
    logFiles1.foreach { f =>
      val raf = new FileOutputStream(f, true).getChannel()
      raf.truncate(1)
      raf.close()
    }

    // Verify that the corrupted files do not prevent reading of the second set of data
    val readData = readDataUsingWriteAheadLog(testDir, closeFileAfterWrite, allowBatching)
    assert(readData === dataToWrite2)
  }

  test(testPrefix + "do not create directories or files unless write") {
    val nonexistentTempPath = File.createTempFile("test", "")
    nonexistentTempPath.delete()
    assert(!nonexistentTempPath.exists())

    val writtenSegment = writeDataManually(generateRandomData(), testFile, allowBatching)
    val wal = createWriteAheadLog(testDir, closeFileAfterWrite, allowBatching)
    assert(!nonexistentTempPath.exists(), "Directory created just by creating log object")
    if (allowBatching) {
      intercept[UnsupportedOperationException](wal.read(writtenSegment.head))
    } else {
      wal.read(writtenSegment.head)
    }
    assert(!nonexistentTempPath.exists(), "Directory created just by attempting to read segment")
  }
}

class FileBasedWriteAheadLogSuite
  extends CommonWriteAheadLogTests(false, false, "FileBasedWriteAheadLog") {

  import WriteAheadLogSuite._

  test("FileBasedWriteAheadLogWriter - writing data") {
    val dataToWrite = generateRandomData()
    val segments = writeDataUsingWriter(testFile, dataToWrite)
    val writtenData = readDataManually(segments)
    assert(writtenData === dataToWrite)
  }

  test("FileBasedWriteAheadLogWriter - syncing of data by writing and reading immediately") {
    val dataToWrite = generateRandomData()
    val writer = new FileBasedWriteAheadLogWriter(testFile, hadoopConf)
    dataToWrite.foreach { data =>
      val segment = writer.write(stringToByteBuffer(data))
      val dataRead = readDataManually(Seq(segment)).head
      assert(data === dataRead)
    }
    writer.close()
  }

  test("FileBasedWriteAheadLogReader - sequentially reading data") {
    val writtenData = generateRandomData()
    writeDataManually(writtenData, testFile, allowBatching = false)
    val reader = new FileBasedWriteAheadLogReader(testFile, hadoopConf)
    val readData = reader.toSeq.map(byteBufferToString)
    assert(readData === writtenData)
    assert(reader.hasNext === false)
    intercept[Exception] {
      reader.next()
    }
    reader.close()
  }

  test("FileBasedWriteAheadLogReader - sequentially reading data written with writer") {
    val dataToWrite = generateRandomData()
    writeDataUsingWriter(testFile, dataToWrite)
    val readData = readDataUsingReader(testFile)
    assert(readData === dataToWrite)
  }

  test("FileBasedWriteAheadLogReader - reading data written with writer after corrupted write") {
    // Write data manually for testing the sequential reader
    val dataToWrite = generateRandomData()
    writeDataUsingWriter(testFile, dataToWrite)
    val fileLength = new File(testFile).length()

    // Append some garbage data to get the effect of a corrupted write
    val fw = new FileWriter(testFile, true)
    fw.append("This line appended to file!")
    fw.close()

    // Verify the data can be read and is same as the one correctly written
    assert(readDataUsingReader(testFile) === dataToWrite)

    // Corrupt the last correctly written file
    val raf = new FileOutputStream(testFile, true).getChannel()
    raf.truncate(fileLength - 1)
    raf.close()

    // Verify all the data except the last can be read
    assert(readDataUsingReader(testFile) === (dataToWrite.dropRight(1)))
  }

  test("FileBasedWriteAheadLogRandomReader - reading data using random reader") {
    // Write data manually for testing the random reader
    val writtenData = generateRandomData()
    val segments = writeDataManually(writtenData, testFile, allowBatching = false)

    // Get a random order of these segments and read them back
    val writtenDataAndSegments = writtenData.zip(segments).toSeq.permutations.take(10).flatten
    val reader = new FileBasedWriteAheadLogRandomReader(testFile, hadoopConf)
    writtenDataAndSegments.foreach { case (data, segment) =>
      assert(data === byteBufferToString(reader.read(segment)))
    }
    reader.close()
  }

  test("FileBasedWriteAheadLogRandomReader- reading data using random reader written with writer") {
    // Write data using writer for testing the random reader
    val data = generateRandomData()
    val segments = writeDataUsingWriter(testFile, data)

    // Read a random sequence of segments and verify read data
    val dataAndSegments = data.zip(segments).toSeq.permutations.take(10).flatten
    val reader = new FileBasedWriteAheadLogRandomReader(testFile, hadoopConf)
    dataAndSegments.foreach { case (data, segment) =>
      assert(data === byteBufferToString(reader.read(segment)))
    }
    reader.close()
  }
}

abstract class CloseFileAfterWriteTests(allowBatching: Boolean, testTag: String)
  extends CommonWriteAheadLogTests(allowBatching, closeFileAfterWrite = true, testTag) {

  import WriteAheadLogSuite._
  test(testPrefix + "close after write flag") {
    // Write data with rotation using WriteAheadLog class
    val numFiles = 3
    val dataToWrite = Seq.tabulate(numFiles)(_.toString)
    // total advance time is less than 1000, therefore log shouldn't be rolled, but manually closed
    writeDataUsingWriteAheadLog(testDir, dataToWrite, closeLog = false, clockAdvanceTime = 100,
      closeFileAfterWrite = true, allowBatching = allowBatching)

    // Read data manually to verify the written data
    val logFiles = getLogFilesInDirectory(testDir)
    assert(logFiles.size === numFiles)
    val writtenData: Seq[String] = readAndDeserializeDataManually(logFiles, allowBatching)
    assert(writtenData === dataToWrite)
  }
}

class FileBasedWriteAheadLogWithFileCloseAfterWriteSuite
  extends CloseFileAfterWriteTests(allowBatching = false, "FileBasedWriteAheadLog")

class BatchedWriteAheadLogSuite extends CommonWriteAheadLogTests(
    allowBatching = true,
    closeFileAfterWrite = false,
    "BatchedWriteAheadLog") with MockitoSugar with BeforeAndAfterEach with Eventually {

  import BatchedWriteAheadLog._
  import WriteAheadLogSuite._

  private var wal: WriteAheadLog = _
  private var walHandle: WriteAheadLogRecordHandle = _
  private var walBatchingThreadPool: ThreadPoolExecutor = _
  private var walBatchingExecutionContext: ExecutionContextExecutorService = _
  private val sparkConf = new SparkConf()

  override def beforeEach(): Unit = {
    wal = mock[WriteAheadLog]
    walHandle = mock[WriteAheadLogRecordHandle]
    walBatchingThreadPool = ThreadUtils.newDaemonFixedThreadPool(8, "wal-test-thread-pool")
    walBatchingExecutionContext = ExecutionContext.fromExecutorService(walBatchingThreadPool)
  }

  override def afterEach(): Unit = {
    if (walBatchingExecutionContext != null) {
      walBatchingExecutionContext.shutdownNow()
    }
  }

  test("BatchedWriteAheadLog - serializing and deserializing batched records") {
    val events = Seq(
      BlockAdditionEvent(ReceivedBlockInfo(0, None, None, null)),
      BatchAllocationEvent(null, null),
      BatchCleanupEvent(Nil)
    )

    val buffers = events.map(e => Record(ByteBuffer.wrap(Utils.serialize(e)), 0L, null))
    val batched = BatchedWriteAheadLog.aggregate(buffers)
    val deaggregate = BatchedWriteAheadLog.deaggregate(batched).map(buffer =>
      Utils.deserialize[ReceivedBlockTrackerLogEvent](buffer.array()))

    assert(deaggregate.toSeq === events)
  }

  test("BatchedWriteAheadLog - failures in wrappedLog get bubbled up") {
    when(wal.write(any[ByteBuffer], anyLong)).thenThrow(new RuntimeException("Hello!"))
    // the BatchedWriteAheadLog should bubble up any exceptions that may have happened during writes
    val batchedWal = new BatchedWriteAheadLog(wal, sparkConf)

    intercept[RuntimeException] {
      val buffer = mock[ByteBuffer]
      batchedWal.write(buffer, 2L)
    }
  }

  // we make the write requests in separate threads so that we don't block the test thread
  private def promiseWriteEvent(wal: WriteAheadLog, event: String, time: Long): Promise[Unit] = {
    val p = Promise[Unit]()
    p.completeWith(Future {
      val v = wal.write(event, time)
      assert(v === walHandle)
    }(walBatchingExecutionContext))
    p
  }

  /**
   * In order to block the writes on the writer thread, we mock the write method, and block it
   * for some time with a promise.
   */
  private def writeBlockingPromise(wal: WriteAheadLog): Promise[Any] = {
    // we would like to block the write so that we can queue requests
    val promise = Promise[Any]()
    when(wal.write(any[ByteBuffer], any[Long])).thenAnswer(
      new Answer[WriteAheadLogRecordHandle] {
        override def answer(invocation: InvocationOnMock): WriteAheadLogRecordHandle = {
          Await.ready(promise.future, 4.seconds)
          walHandle
        }
      }
    )
    promise
  }

  test("BatchedWriteAheadLog - name log with aggregated entries with the timestamp of last entry") {
    val batchedWal = new BatchedWriteAheadLog(wal, sparkConf)
    // block the write so that we can batch some records
    val promise = writeBlockingPromise(wal)

    val event1 = "hello"
    val event2 = "world"
    val event3 = "this"
    val event4 = "is"
    val event5 = "doge"

    // The queue.take() immediately takes the 3, and there is nothing left in the queue at that
    // moment. Then the promise blocks the writing of 3. The rest get queued.
    promiseWriteEvent(batchedWal, event1, 3L)
    // rest of the records will be batched while it takes 3 to get written
    promiseWriteEvent(batchedWal, event2, 5L)
    promiseWriteEvent(batchedWal, event3, 8L)
    promiseWriteEvent(batchedWal, event4, 12L)
    promiseWriteEvent(batchedWal, event5, 10L)
    eventually(timeout(1 second)) {
      assert(walBatchingThreadPool.getActiveCount === 5)
    }
    promise.success(true)

    val buffer1 = wrapArrayArrayByte(Array(event1))
    val buffer2 = wrapArrayArrayByte(Array(event2, event3, event4, event5))

    eventually(timeout(1 second)) {
      verify(wal, times(1)).write(meq(buffer1), meq(3L))
      // the file name should be the timestamp of the last record, as events should be naturally
      // in order of timestamp, and we need the last element.
      verify(wal, times(1)).write(meq(buffer2), meq(10L))
    }
  }

  test("BatchedWriteAheadLog - shutdown properly") {
    val batchedWal = new BatchedWriteAheadLog(wal, sparkConf)
    batchedWal.close()
    verify(wal, times(1)).close()

    intercept[IllegalStateException](batchedWal.write(mock[ByteBuffer], 12L))
  }

  test("BatchedWriteAheadLog - fail everything in queue during shutdown") {
    val batchedWal = new BatchedWriteAheadLog(wal, sparkConf)

    // block the write so that we can batch some records
    writeBlockingPromise(wal)

    val event1 = ("hello", 3L)
    val event2 = ("world", 5L)
    val event3 = ("this", 8L)
    val event4 = ("is", 9L)
    val event5 = ("doge", 10L)

    // The queue.take() immediately takes the 3, and there is nothing left in the queue at that
    // moment. Then the promise blocks the writing of 3. The rest get queued.
    val writePromises = Seq(event1, event2, event3, event4, event5).map { event =>
      promiseWriteEvent(batchedWal, event._1, event._2)
    }

    eventually(timeout(1 second)) {
      assert(walBatchingThreadPool.getActiveCount === 5)
    }

    batchedWal.close()
    eventually(timeout(1 second)) {
      assert(writePromises.forall(_.isCompleted))
      assert(writePromises.forall(_.future.value.get.isFailure)) // all should have failed
    }
  }
}

class BatchedWriteAheadLogWithCloseFileAfterWriteSuite
  extends CloseFileAfterWriteTests(allowBatching = true, "BatchedWriteAheadLog")

object WriteAheadLogSuite {

  private val hadoopConf = new Configuration()

  /** Write data to a file directly and return an array of the file segments written. */
  def writeDataManually(
      data: Seq[String],
      file: String,
      allowBatching: Boolean): Seq[FileBasedWriteAheadLogSegment] = {
    val segments = new ArrayBuffer[FileBasedWriteAheadLogSegment]()
    val writer = HdfsUtils.getOutputStream(file, hadoopConf)
    def writeToStream(bytes: Array[Byte]): Unit = {
      val offset = writer.getPos
      writer.writeInt(bytes.size)
      writer.write(bytes)
      segments += FileBasedWriteAheadLogSegment(file, offset, bytes.size)
    }
    if (allowBatching) {
      writeToStream(wrapArrayArrayByte(data.toArray[String]).array())
    } else {
      data.foreach { item =>
        writeToStream(Utils.serialize(item))
      }
    }
    writer.close()
    segments
  }

  /**
   * Write data to a file using the writer class and return an array of the file segments written.
   */
  def writeDataUsingWriter(
      filePath: String,
      data: Seq[String]): Seq[FileBasedWriteAheadLogSegment] = {
    val writer = new FileBasedWriteAheadLogWriter(filePath, hadoopConf)
    val segments = data.map {
      item => writer.write(item)
    }
    writer.close()
    segments
  }

  /** Write data to rotating files in log directory using the WriteAheadLog class. */
  def writeDataUsingWriteAheadLog(
      logDirectory: String,
      data: Seq[String],
      closeFileAfterWrite: Boolean,
      allowBatching: Boolean,
      manualClock: ManualClock = new ManualClock,
      closeLog: Boolean = true,
      clockAdvanceTime: Int = 500): WriteAheadLog = {
    if (manualClock.getTimeMillis() < 100000) manualClock.setTime(10000)
    val wal = createWriteAheadLog(logDirectory, closeFileAfterWrite, allowBatching)

    // Ensure that 500 does not get sorted after 2000, so put a high base value.
    data.foreach { item =>
      manualClock.advance(clockAdvanceTime)
      wal.write(item, manualClock.getTimeMillis())
    }
    if (closeLog) wal.close()
    wal
  }

  /** Read data from a segments of a log file directly and return the list of byte buffers. */
  def readDataManually(segments: Seq[FileBasedWriteAheadLogSegment]): Seq[String] = {
    segments.map { segment =>
      val reader = HdfsUtils.getInputStream(segment.path, hadoopConf)
      try {
        reader.seek(segment.offset)
        val bytes = new Array[Byte](segment.length)
        reader.readInt()
        reader.readFully(bytes)
        val data = Utils.deserialize[String](bytes)
        reader.close()
        data
      } finally {
        reader.close()
      }
    }
  }

  /** Read all the data from a log file directly and return the list of byte buffers. */
  def readDataManually[T](file: String): Seq[T] = {
    val reader = HdfsUtils.getInputStream(file, hadoopConf)
    val buffer = new ArrayBuffer[T]
    try {
      while (true) {
        // Read till EOF is thrown
        val length = reader.readInt()
        val bytes = new Array[Byte](length)
        reader.read(bytes)
        buffer += Utils.deserialize[T](bytes)
      }
    } catch {
      case ex: EOFException =>
    } finally {
      reader.close()
    }
    buffer
  }

  /** Read all the data from a log file using reader class and return the list of byte buffers. */
  def readDataUsingReader(file: String): Seq[String] = {
    val reader = new FileBasedWriteAheadLogReader(file, hadoopConf)
    val readData = reader.toList.map(byteBufferToString)
    reader.close()
    readData
  }

  /** Read all the data in the log file in a directory using the WriteAheadLog class. */
  def readDataUsingWriteAheadLog(
      logDirectory: String,
      closeFileAfterWrite: Boolean,
      allowBatching: Boolean): Seq[String] = {
    val wal = createWriteAheadLog(logDirectory, closeFileAfterWrite, allowBatching)
    val data = wal.readAll().asScala.map(byteBufferToString).toSeq
    wal.close()
    data
  }

  /** Get the log files in a directory. */
  def getLogFilesInDirectory(directory: String): Seq[String] = {
    val logDirectoryPath = new Path(directory)
    val fileSystem = HdfsUtils.getFileSystemForPath(logDirectoryPath, hadoopConf)

    if (fileSystem.exists(logDirectoryPath) && fileSystem.getFileStatus(logDirectoryPath).isDir) {
      fileSystem.listStatus(logDirectoryPath).map { _.getPath() }.sortBy {
        _.getName().split("-")(1).toLong
      }.map {
        _.toString.stripPrefix("file:")
      }
    } else {
      Seq.empty
    }
  }

  def createWriteAheadLog(
      logDirectory: String,
      closeFileAfterWrite: Boolean,
      allowBatching: Boolean): WriteAheadLog = {
    val sparkConf = new SparkConf
    val wal = new FileBasedWriteAheadLog(sparkConf, logDirectory, hadoopConf, 1, 1,
      closeFileAfterWrite)
    if (allowBatching) new BatchedWriteAheadLog(wal, sparkConf) else wal
  }

  def generateRandomData(): Seq[String] = {
    (1 to 100).map { _.toString }
  }

  def readAndDeserializeDataManually(logFiles: Seq[String], allowBatching: Boolean): Seq[String] = {
    if (allowBatching) {
      logFiles.flatMap { file =>
        val data = readDataManually[Array[Array[Byte]]](file)
        data.flatMap(byteArray => byteArray.map(Utils.deserialize[String]))
      }
    } else {
      logFiles.flatMap { file => readDataManually[String](file)}
    }
  }

  implicit def stringToByteBuffer(str: String): ByteBuffer = {
    ByteBuffer.wrap(Utils.serialize(str))
  }

  implicit def byteBufferToString(byteBuffer: ByteBuffer): String = {
    Utils.deserialize[String](byteBuffer.array)
  }

  def wrapArrayArrayByte[T](records: Array[T]): ByteBuffer = {
    ByteBuffer.wrap(Utils.serialize[Array[Array[Byte]]](records.map(Utils.serialize[T])))
  }
}
