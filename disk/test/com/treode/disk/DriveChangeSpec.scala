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

import com.treode.jackson.JsonReader
import com.treode.jackson.messages._
import com.treode.notify.{Message, Notification}
import org.scalatest.FreeSpec

class DriveChangeSpec extends FreeSpec {

  def assertFromJson (a: DriveChange) (s: String): Unit =
    assertResult (Notification.result (a)) (DriveChange.fromJson (JsonReader (s)))

  def assertFromJsonErrors (expected: Message*) (s: String): Unit =
    assertResult (Notification.Errors (expected)) (DriveChange.fromJson (JsonReader (s)))

  val attach = DriveAttachment (Paths.get ("/a/b/c"), DriveGeometry (30, 13, 1L << 40))

  val drain = Paths.get ("/a/b/c")

  val attachJson = """{
    "path": "/a/b/c",
    "geometry": {"segmentBits": 30, "blockBits": 13, "diskBytes": 1099511627776}
  }"""

  val drainJson = """ "/a/b/c" """

  "fromJson with a valid JSON object should" - {

    "parse some attaches and drains" in {
      assertFromJson {
        DriveChange (Seq (attach), Seq (drain))
      } {
        s"""{"attach": [$attachJson], "drain": [$drainJson]}"""
      }}

    "parse some attaches and no drains" in {
      assertFromJson {
        DriveChange (Seq (attach), Seq.empty)
      } {
        s"""{"attach": [$attachJson]}"""
      }}

    "parse no attaches and some drains" in {
      assertFromJson {
        DriveChange (Seq.empty, Seq (drain))
      } {
        s"""{"drain": [$drainJson]}"""
      }}

    "parse no attaches and no drains" in {
      assertFromJson {
        DriveChange (Seq.empty, Seq.empty)
      } {
        """{}"""
      }}

    "expect an attachment to be valid" in {
      assertFromJsonErrors (ExpectedObject ("attach/0")) {
        """{"attach": [1]}"""
      }}

    "expect a drain to be valid" in {
      assertFromJsonErrors (ExpectedFilePath ("drain/0")) {
        """{"drain": [1]}"""
      }}}}
