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
import com.treode.disk.{RecordDescriptor, RecordRegistry}
import com.treode.pickle.{Picklers, PicklerRegistry}

class LogReaderSpec extends FlatSpec {

  private val rec = RecordDescriptor (10, Picklers.string)
  private val geom = DriveGeometry (10, 10, 16384)

  "LogReader" should "read one batch from a File" in {
    implicit val scheduler = StubScheduler.random()
    val testfile = StubFile (1 << 16, 0)
    val reg = new RecordRegistry
    val builder = Seq.newBuilder [String]
    reg.replay(rec)(builder += _)
    val reader = new LogReader (testfile, geom, reg)

    val buf = ArrayBuffer.writable (testfile.data)
    val str = "str"
    buf.writeInt (str.length() + 8 + 1)
    buf.writeInt (1)
    buf.writeLong(10)
    buf.writeString (str)
    buf.writeByte (0)

    val vs = reader.read().expectPass()
    vs foreach (_())
    val result = builder.result()
    assert (result (0) == str)
  }

  "LogReader" should "read two batches from a File" in {
    implicit val scheduler = StubScheduler.random()
    val testfile = StubFile (1 << 16, 0)
    val reg = new RecordRegistry
    val builder = Seq.newBuilder [String]
    reg.replay(rec)(builder += _)
    val reader = new LogReader (testfile, geom, reg)

    val buf = ArrayBuffer.writable (testfile.data)
    val str = "str"
    buf.writeInt(str.length() + 8 + 1)
    buf.writeInt (1)
    buf.writeLong(10)
    buf.writeString (str)
    buf.writeByte (1)
    val strTwo = "strTwo"
    val strThree = "strThree"
    buf.writeInt (strTwo.length() + 8 + 1 + strThree.length() + 8 + 1)
    buf.writeInt (2)
    buf.writeLong(10)
    buf.writeString (strTwo)
    buf.writeLong(10)
    buf.writeString (strThree)
    buf.writeByte (0)

    val vs = reader.read().expectPass()
    vs foreach (_())
    val result = builder.result()
    assert (result (0) == str)
    assert (result (1) == strTwo)
    assert (result (2) == strThree)
  }}
