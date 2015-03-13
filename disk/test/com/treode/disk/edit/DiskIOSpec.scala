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
import com.treode.disk.DiskTestConfig
import org.scalatest.FlatSpec

class DiskIOSpec extends FlatSpec {

  implicit val config = DiskTestConfig()

  "The PageReader" should "be able to read the string the PageWriter wrote" in {
    implicit val scheduler = StubScheduler.random()
    val a = "this is a string"
    val readPos = 0
    val f = StubFile (1 << 20, 0)

    val dw = new PageWriter (f)
    val dr = new PageReader (f)
    val (pos, len) = dw.write (a) .expectPass()
    val readString = dr.readString (readPos, len) .expectPass()
    assert (a.equals (readString))
  }

  it should "write and read multiple times to disk" in {
    implicit val scheduler = StubScheduler.random()
    val a = "abcdef"
    val b = "123456789"
    val startPos = 0
    val f = StubFile (1 << 20, 0)

    val dw = new PageWriter (f)
    val dr = new PageReader (f)
    val (posA, lenA) = dw.write (a) .expectPass()
    val (posB, lenB) = dw.write (b) .expectPass()
    val readA = dr.readString (startPos, lenA) .expectPass()
    val readB = dr.readString (posA,     lenB) .expectPass()
    assert (a.equals (readA))
    assert (b.equals (readB))
    assert (posA == startPos + lenA)
    assert (posB == posA + lenB)
  }

  it should "write and read correctly out of order" in {
    implicit val scheduler = StubScheduler.random()
    val a = "abcdef"
    val b = "123456789"
    val startPos = 0
    val f = StubFile (1 << 20, 0)

    val dw = new PageWriter (f)
    val dr = new PageReader (f)
    val (posA, lenA) = dw.write (a) .expectPass()
    val (posB, lenB) = dw.write (b) .expectPass()
    val readB = dr.readString (posA,     lenB) .expectPass()
    val readA = dr.readString (startPos, lenA) .expectPass()
    assert (a.equals (readA))
    assert (b.equals (readB))
    assert (posA == startPos + lenA)
    assert (posB == posA + lenB)
  }
}
