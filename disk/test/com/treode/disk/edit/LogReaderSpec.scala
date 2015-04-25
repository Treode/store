/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.treode.disk.edit

import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.buffer.ArrayBuffer
import com.treode.disk.DriveGeometry
import org.scalatest.FlatSpec

class LogReaderSpec extends FlatSpec {

  "LogReader" should "read one batch from a File" in {
    implicit val scheduler = StubScheduler.random()
    val testfile = StubFile (1 << 16, 0)
    val logreader = new LogReader (testfile, DriveGeometry (10, 10, 16384))

    val buf = ArrayBuffer.writable (testfile.data)
    val str = "str"
    buf.writeInt (str.length() + 1)
    buf.writeInt (1)
    buf.writeString (str)
    buf.writeByte (0)

    val readStr = logreader.read().expectPass()
    assert (readStr (0) == str)
  }

  "LogReader" should "read two batches from a File" in {
    implicit val scheduler = StubScheduler.random()
    val testfile = StubFile (1 << 16, 0)
    val logreader = new LogReader (testfile, DriveGeometry (10, 10, 16384))

    val buf = ArrayBuffer.writable (testfile.data)
    val str = "str"
    buf.writeInt(str.length() + 1)
    buf.writeInt (1)
    buf.writeString (str)
    buf.writeByte (1)
    val strTwo = "strTwo"
    val strThree = "strThree"
    buf.writeInt (strTwo.length() + 1 + strThree.length() + 1)
    buf.writeInt (2)
    buf.writeString (strTwo)
    buf.writeString (strThree)
    buf.writeByte (0)

    val readStr = logreader.read().expectPass()
    assert (readStr (0) == str)
    assert (readStr (1) == strTwo)
    assert (readStr (2) == strThree)
  }
}
