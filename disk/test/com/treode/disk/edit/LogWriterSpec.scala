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

class LogWriterSpec extends FlatSpec {

  "LogWriter" should "record one batch of one string to a File" in {
    implicit val scheduler = StubScheduler.random()
    val testfile = StubFile (1 << 16, 0)
    val dsp = new LogDispatcher
    val rec = new LogWriter (testfile, DriveGeometry (10, 10, 16384), dsp)

    val str = "str"
    dsp.record (str) .expectPass()
    
    val input = ArrayBuffer.readable(testfile.data)
    // For our test strings, 1 char = 1 byte, + 1 byte for len.
    assert (input.readInt() == str.length() + 1)
    assert (input.readInt() == 1)
    assert (input.readString() == str)
    assert (input.readByte() == 0)
  }

  "LogWriter" should "record two batches of strings to a File" in {
    implicit val scheduler = StubScheduler.random()
    val testfile = StubFile(1 << 16, 0)
    val dsp = new LogDispatcher
    val rec = new LogWriter(testfile, DriveGeometry(10, 10, 16384), dsp)

    val str = "str"
    val cb1 = dsp.record (str) .capture()
    val strTwo = "strTwo"
    val cb2 = dsp.record (strTwo) .capture()
    val strThree = "strThree"
    val cb3 = dsp.record (strThree) .capture()
    scheduler.run()
    cb1.assertPassed()
    cb2.assertPassed()
    cb3.assertPassed()

    val input = ArrayBuffer.readable(testfile.data)
    // For our test strings, 1 char = 1 byte, + 1 byte for len.
    // first batch of a single string
    assert (input.readInt() == str.length() + 1)
    assert (input.readInt() == 1)
    assert (input.readString() == str)
    assert (input.readByte() == 1)
    // second batch of two strings
    assert (input.readInt() == strTwo.length() + 1 + strThree.length() + 1)
    assert (input.readInt() == 2)
    assert (input.readString() == strTwo)
    assert (input.readString() == strThree)
    assert (input.readByte() == 0)
  }
}
