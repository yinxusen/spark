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

import java.io.{DataInputStream, EOFException, RandomAccessFile}
import java.net.{InetAddress, Socket}
import java.nio.channels.FileChannel

import io.netty.buffer.ArrowBuf
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.file.ArrowReader
import org.apache.arrow.vector.schema.ArrowRecordBatch

import org.apache.spark.sql.test.SharedSQLContext

case class ArrowIntTest(a: Int, b: Int)

class DatasetToArrowSuite extends QueryTest with SharedSQLContext {

  import testImplicits._

  test("Collect as arrow to python") {

    val ds = Seq(ArrowIntTest(1, 2), ArrowIntTest(2, 3), ArrowIntTest(3, 4)).toDS()

    val port = ds.collectAsArrowToPython()

    val clientThread: Thread = new Thread(new Runnable() {
      def run() {
        try {
          val receiver: RecordBatchReceiver = new RecordBatchReceiver
          val record: ArrowRecordBatch = receiver.read(port)
        }
          catch {
            case e: Exception =>
              throw e
          }
        }
    })

    clientThread.start()

    try {
      clientThread.join()
    } catch {
      case e: InterruptedException =>
        throw e
      case _ =>
    }
  }
}

class RecordBatchReceiver {

  def array(buf: ArrowBuf): Array[Byte] = {
    val bytes = Array.ofDim[Byte](buf.readableBytes())
    buf.readBytes(bytes)
    bytes
  }

  def connectAndRead(port: Int): (Array[Byte], Int) = {
    val s = new Socket(InetAddress.getByName("localhost"), port)
    val is = s.getInputStream

    val dis = new DataInputStream(is)
    val len = dis.readInt()

    val buffer = Array.ofDim[Byte](len)
    val bytesRead = dis.read(buffer)
    if (bytesRead != len) {
      throw new EOFException("Wrong EOF")
    }
    (buffer, len)
  }

  def makeFile(buffer: Array[Byte]): FileChannel = {
    var aFile = new RandomAccessFile("/tmp/nio-data.txt", "rw")
    aFile.write(buffer)
    aFile.close()

    aFile = new RandomAccessFile("/tmp/nio-data.txt", "r")
    val fChannel = aFile.getChannel
    fChannel
  }

  def readRecordBatch(fc: FileChannel, len: Int): ArrowRecordBatch = {
    val allocator = new RootAllocator(len)
    val reader = new ArrowReader(fc, allocator)
    val footer = reader.readFooter()
    val schema = footer.getSchema
    val blocks = footer.getRecordBatches
    val recordBatch = reader.readRecordBatch(blocks.get(0))
    recordBatch
  }

  def read(port: Int): ArrowRecordBatch = {
    val (buffer, len) = connectAndRead(port)
    val fc = makeFile(buffer)
    readRecordBatch(fc, len)
  }
}
