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

package com.treode.disk

import com.treode.disk.messages._
import com.treode.jackson.JsonReader
import com.treode.jackson.messages._
import com.treode.notify.{Message, Notification}
import org.scalatest.FlatSpec

class DriveGeometrySpec extends FlatSpec {

  implicit val config = DiskTestConfig (superBlockBits = 12)

  val block = 1 << 12
  val seg = 1 << 16

  def assertBounds (id: Int, pos: Long, limit: Long) (actual: SegmentBounds): Unit =
    assertResult (SegmentBounds (id, pos, limit)) (actual)

  def assertFromJson (g: DriveGeometry) (s: String): Unit =
    assertResult (Notification.result (g)) (DriveGeometry.fromJson (JsonReader (s)))

  def assertFromJsonErrors (ms: Message*) (s: String): Unit =
    assertResult (Notification.Errors (ms)) (DriveGeometry.fromJson (JsonReader (s)))

  "DriveGeometry" should "compute the segment count" in {
    val disk1 = 1 << 20
    val disk2 = 1 << 21
    def c (diskBytes: Long) = DriveGeometry (16, 12, diskBytes).segmentCount
    assertResult (16) (c (disk1))
    assertResult (17) (c (disk1 + 4*block))
    assertResult (32) (c (disk2))
    assertResult (32) (c (disk2 - seg + 4*block))
    assertResult (31) (c (disk2 - seg + 4*block - 1))
    assertResult (32) (c (disk2 + 4*block - 1))
    assertResult (33) (c (disk2 + 4*block))
  }

  it should "align block length" in {
    val c = DriveGeometry (16, 12, 1 << 20)

    assertResult (0) (c.blockAlignUp (0))
    assertResult (block) (c.blockAlignUp (1))
    assertResult (block) (c.blockAlignUp (4095))
    assertResult (block) (c.blockAlignUp (4096))
    assertResult (2*block) (c.blockAlignUp (4097))

    assertResult (0) (c.blockAlignDown (0))
    assertResult (0) (c.blockAlignDown (1))
    assertResult (0) (c.blockAlignDown (4095))
    assertResult (block) (c.blockAlignDown (4096))
    assertResult (block) (c.blockAlignDown (4097))
  }

  it should "compute the segment bounds" in {
    val c = DriveGeometry (16, 12, (1 << 20) + 6*block)
    assertBounds (0, config.diskLeadBytes, seg) (c.segmentBounds (0))
    assertBounds (1, seg, 2*seg) (c.segmentBounds (1))
    assertBounds (2, 2*seg, 3*seg) (c.segmentBounds (2))
    assertBounds (2, 2*seg, 3*seg) (c.segmentBounds (2))
    assertBounds (16, 16*seg, 16*seg + 6*block) (c.segmentBounds (16))
  }

  "fromJson" should "parse a valid JSON object" in {
    assertFromJson (DriveGeometry (30, 13, 1L << 40)) {
      """{"segmentBits": 30, "blockBits": 13, "diskBytes": 1099511627776}"""
    }}

  it should "expect valid values" in {
    assertFromJsonErrors (SegmentNeeds16Blocks ("")) {
      """{"segmentBits": 13, "blockBits": 13, "diskBytes": 1099511627776}"""
    }
    assertFromJsonErrors (DriveNeeds16Segments ("")) {
      """{"segmentBits": 30, "blockBits": 13, "diskBytes": 1073741824}"""
    }}

  it should "expect a JSON object" in {
    val error = ExpectedObject ("")
    assertFromJsonErrors (error) ("1")
    assertFromJsonErrors (error) ("\"s\"")
    assertFromJsonErrors (error) ("[]")
  }

  it should "expect segmentBits to be a positive int" in {
    val error = ExpectedNumber ("segmentBits", 1, Int.MaxValue)
    assertFromJsonErrors (error) ("""{"blockBits": 1, "diskBytes": 1}""")
    assertFromJsonErrors (error) ("""{"segmentBits": "", "blockBits": 1, "diskBytes": 1}""")
    assertFromJsonErrors (error) ("""{"segmentBits": {}, "blockBits": 1, "diskBytes": 1}""")
    assertFromJsonErrors (error) ("""{"segmentBits": [], "blockBits": 1, "diskBytes": 1}""")
    assertFromJsonErrors (error) ("""{"segmentBits": -1, "blockBits": 1, "diskBytes": 1}""")
    assertFromJsonErrors (error) ("""{"segmentBits": 2147483649, "blockBits": 1, "diskBytes": 1}""")
  }

  it should "expect blockBits to be a positive int" in {
     val error = ExpectedNumber ("blockBits", 1, Int.MaxValue)
    assertFromJsonErrors (error) ("""{"segmentBits": 1, "diskBytes": 1}""")
    assertFromJsonErrors (error) ("""{"blockBits": "", "segmentBits": 1, "diskBytes": 1}""")
    assertFromJsonErrors (error) ("""{"blockBits": {}, "segmentBits": 1, "diskBytes": 1}""")
    assertFromJsonErrors (error) ("""{"blockBits": [], "segmentBits": 1, "diskBytes": 1}""")
    assertFromJsonErrors (error) ("""{"blockBits": -1, "segmentBits": 1, "diskBytes": 1}""")
    assertFromJsonErrors (error) ("""{"blockBits": 2147483649, "segmentBits": 1, "diskBytes": 1}""")
  }

  it should "expect diskBytes to be a positive int" in {
    val error = ExpectedNumber ("diskBytes", 1, Long.MaxValue)
    assertFromJsonErrors (error) ("""{"segmentBits": 1, "blockBits": 1}""")
    assertFromJsonErrors (error) ("""{"diskBytes": "", "segmentBits": 1, "blockBits": 1}""")
    assertFromJsonErrors (error) ("""{"diskBytes": {}, "segmentBits": 1, "blockBits": 1}""")
    assertFromJsonErrors (error) ("""{"diskBytes": [], "segmentBits": 1, "blockBits": 1}""")
    assertFromJsonErrors (error) ("""{"diskBytes": -1, "segmentBits": 1, "blockBits": 1}""")
    assertFromJsonErrors (error) ("""{"diskBytes": 9223372036854775809, "segmentBits": 1, "blockBits": 1}""")
  }}
