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

import java.nio.file.Paths

import com.treode.disk.messages._
import com.treode.jackson.JsonReader
import com.treode.notify.{Message, Notification}
import org.scalatest.FlatSpec

class DriveAttachmentSpec extends FlatSpec {

  def assertFromJson (a: DriveAttachment) (s: String): Unit =
    assertResult (Notification.result (a)) (DriveAttachment.fromJson (JsonReader (s)))

  def assertFromJsonErrors (expected: Message*) (s: String): Unit =
    assertResult (Notification.Errors (expected)) (DriveAttachment.fromJson (JsonReader (s)))

  "fromJson" should "parse a valid JSON object" in {
    assertFromJson {
      DriveAttachment (
        Paths.get ("/a/b/c"),
        DriveGeometry (30, 13, 1L << 40))
    } {
      """{"path": "/a/b/c",
          "geometry": {"segmentBits": 30, "blockBits": 13, "diskBytes": 1099511627776}
      }"""
    }}

  it should "expect geometry to be valid" in {
    assertFromJsonErrors (DriveNeeds16Segments ("geometry")) {
      """{"path": "/a/b/c",
          "geometry": {"segmentBits": 30, "blockBits": 13, "diskBytes": 1073741824}
      }"""
    }}}
