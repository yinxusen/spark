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

package org.apache.spark.sql

import java.io._
import java.net.{InetAddress, Socket}
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.channels.FileChannel

import scala.util.Random

import io.netty.buffer.ArrowBuf
import org.apache.arrow.flatbuf.Precision
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.file.ArrowReader
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}

import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils


class DatasetToArrowPerfSuite extends QueryTest with SharedSQLContext {

  import testImplicits._
  import DatasetToArrowPerfSuite.time

  final val numElements = 4 * 100000
  @transient var data: Dataset[ArrowTestClass] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    data = Seq.fill(numElements)(ArrowTestClass(
      Random.nextInt, Random.nextDouble, Random.nextString(Random.nextInt(100)))).toDS().cache()
    data.collect()
  }

  test("Performance test of Arrow") {
    var numBytesRead = 0
    time {
      val port = data.collectAsArrowToPython()
      val receiver: RecordBatchReceiver = new RecordBatchReceiver
      val (buffer, cnt) = receiver.connectAndRead(port)
      numBytesRead = cnt
      val channel = receiver.makeFile(buffer)
    }("Arrow")
    // scalastyle:off println
    println(s"Size of Arrow is $numBytesRead")
    // scalastyle:on println
  }

  test("Performance test of AutoBatchPickler") {
    var numBytesRead = 0
    time {
      val port = data.collectToPython()
      val receiver: RecordBatchReceiver = new RecordBatchReceiver
      val (buffer, cnt) = receiver.connectAndReadPickler(port)
      numBytesRead = cnt
      val channel = receiver.makeFile(buffer)
    }("AutoBatchPickler")
    // scalastyle:off println
    println(s"Size of AutoBatchPickler is $numBytesRead")
    // scalastyle:on println
  }

}

object DatasetToArrowPerfSuite {
  def compareSchemaTypes(arrowField: Field, sparkField: StructField): Boolean = {
    val arrowType = arrowField.getType
    val sparkType = sparkField.dataType
    (arrowType, sparkType) match {
      case (_: ArrowType.Int, _: IntegerType) => true
      case (_: ArrowType.FloatingPoint, _: DoubleType) =>
        arrowType.asInstanceOf[ArrowType.FloatingPoint].getPrecision == Precision.DOUBLE
      case (_: ArrowType.FloatingPoint, _: FloatType) =>
        arrowType.asInstanceOf[ArrowType.FloatingPoint].getPrecision == Precision.SINGLE
      case (_: ArrowType.List, _: StringType) =>
        val subField = arrowField.getChildren
        (subField.size() == 1) && subField.get(0).getType.isInstanceOf[ArrowType.Utf8]
      case (_: ArrowType.Bool, _: BooleanType) => true
      case _ => false
    }
  }

  def time[R](block: => R)(blockName: String = "Unknown Block"): R = {
    val t0 = System.nanoTime()
    val result: R = block
    val t1 = System.nanoTime()
    // scalastyle:off println
    println(s"Elapsed time of $blockName is ${(t1 - t0) * 1.0 / 1000 / 1000} ms.")
    // scalastyle:on println
    result
  }
}

/*
class RecordBatchReceiver {

  val allocator = new RootAllocator(Long.MaxValue)

  def getIntArray(buf: ArrowBuf): Array[Int] = {
    val buffer = ByteBuffer.wrap(array(buf)).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer()
    val resultArray = Array.ofDim[Int](buffer.remaining())
    buffer.get(resultArray)
    resultArray
  }

  def getDoubleArray(buf: ArrowBuf): Array[Double] = {
    val buffer = ByteBuffer.wrap(array(buf)).order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer()
    val resultArray = Array.ofDim[Double](buffer.remaining())
    buffer.get(resultArray)
    resultArray
  }

  def getStringArray(bufOffsets: ArrowBuf, bufValues: ArrowBuf): Array[UTF8String] = {
    val offsets = getIntArray(bufOffsets)
    val lens = offsets.zip(offsets.drop(1))
      .map { case (prevOffset, offset) => offset - prevOffset }

    val values = array(bufValues)
    val strings = offsets.zip(lens).map { case (offset, len) =>
      UTF8String.fromBytes(values, offset, len)
    }
    strings
  }

  private def array(buf: ArrowBuf): Array[Byte] = {
    val bytes = Array.ofDim[Byte](buf.readableBytes())
    buf.readBytes(bytes)
    bytes
  }

  def connectAndRead(port: Int): (Array[Byte], Int) = {
    val clientSocket = new Socket(InetAddress.getByName("localhost"), port)
    val clientDataIns = new DataInputStream(clientSocket.getInputStream)
    val messageLength = clientDataIns.readInt()
    val buffer = Array.ofDim[Byte](messageLength)
    clientDataIns.readFully(buffer, 0, messageLength)
    (buffer, messageLength)
  }

  def makeFile(buffer: Array[Byte]): FileChannel = {
    val tempDir = Utils.createTempDir(namePrefix = this.getClass.getName).getPath
    val arrowFile = new File(tempDir, "arrow-bytes")
    val arrowOus = new FileOutputStream(arrowFile.getPath)
    arrowOus.write(buffer)
    arrowOus.close()

    val arrowIns = new FileInputStream(arrowFile.getPath)
    arrowIns.getChannel
  }
}
*/
