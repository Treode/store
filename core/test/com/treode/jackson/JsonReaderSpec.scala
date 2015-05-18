/*
 * Copyright 2015 Treode, Inc.
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

package com.treode.jackson

import java.nio.file.Paths

import com.treode.jackson.messages._
import com.treode.notify.{Message, Notification}
import org.scalatest.FlatSpec

class JsonReaderSpec extends FlatSpec {

  val expectedFrom0to2 = ExpectedNumber ("n", 0, 2)
  val expectedObject = ExpectedObject ("obj")
  val expectedPath = ExpectedFilePath ("p")
  val path = Paths.get ("/a/b/c")

  def getFilePath (s: String) =
    JsonReader (s) .getFilePath ("p")

  def getInt (s: String, lower: Int = Int.MinValue, upper: Int = Int.MaxValue) =
    JsonReader (s) .getInt ("n", lower, upper)

  def getLong (s: String, lower: Long = Long.MinValue, upper: Long = Long.MaxValue) =
    JsonReader (s) .getLong ("n", lower, upper)

  def getObject (s: String) =
    JsonReader (s) .getObject ("obj")

  def assertErrors [A] (expected: Message*) (actual: Notification [A]): Unit =
    assertResult (Notification.Errors (expected)) (actual)

  "getFilePath" should "read a valid path" in {
    assertResult (path) (getFilePath ("""{"p": "/a/b/c"}""") .get)
  }

  it should "report a missing field" in {
    assertErrors (expectedPath) (getFilePath ("""{}"""))
  }

  it should "report a non-string field" in {
    assertErrors (expectedPath) (getFilePath ("""{"p": 1}"""))
    assertErrors (expectedPath) (getFilePath ("""{"p": []}"""))
    assertErrors (expectedPath) (getFilePath ("""{"p": {}}"""))
  }

  "getInt" should "read an valid int field" in {
    assertResult (0) (getInt ("""{"n": 0}""") .get)
    assertResult (Int.MinValue) (getInt ("""{"n": -2147483648}""") .get)
    assertResult (Int.MaxValue) (getInt ("""{"n": 2147483647}""") .get)
  }

  it should "report a missing field" in {
    assertErrors (expectedFrom0to2) (getInt ("""{}""", 0, 2))
  }

  it should "report a non-integer field" in {
    assertErrors (expectedFrom0to2) (getInt ("""{"n": "hello"}""", 0, 2))
    assertErrors (expectedFrom0to2) (getInt ("""{"n": "[]"}""", 0, 2))
    assertErrors (expectedFrom0to2) (getInt ("""{"n": "{}"}""", 0, 2))
  }

  it should "report a value out of range" in {
    assertErrors (expectedFrom0to2) (getInt ("""{"n": -1}""", 0, 2))
    assertErrors (expectedFrom0to2) (getInt ("""{"n": 3}""", 0, 2))
  }

  "getLong" should "read an valid long field" in {
    assertResult (0) (getLong ("""{"n": 0}""") .get)
    assertResult (Int.MinValue) (getLong ("""{"n": -2147483648}""") .get)
    assertResult (Int.MaxValue) (getLong ("""{"n": 2147483647}""") .get)
    assertResult (Long.MinValue) (getLong ("""{"n": -9223372036854775808}""") .get)
    assertResult (Long.MaxValue) (getLong ("""{"n": 9223372036854775807}""") .get)
  }

  it should "report a missing field" in {
    assertErrors (expectedFrom0to2) (getLong ("""{}""", 0, 2))
  }

  it should "report a non-integer field" in {
    assertErrors (expectedFrom0to2) (getLong ("""{"n": "hello"}""", 0, 2))
    assertErrors (expectedFrom0to2) (getLong ("""{"n": "[]"}""", 0, 2))
    assertErrors (expectedFrom0to2) (getLong ("""{"n": "{}"}""", 0, 2))
  }

  it should "report a value out of range" in {
    assertErrors (expectedFrom0to2) (getLong ("""{"n": -1}""", 0, 2))
    assertErrors (expectedFrom0to2) (getLong ("""{"n": 3}""", 0, 2))
  }

  "getObject" should "read a valid object" in {
    assert (getObject ("""{"obj": {}}""") .get.isInstanceOf [JsonReader])
  }

  it should "report a missing field" in {
    assertErrors (expectedObject) (getObject ("""{}"""))
  }

  it should "report a non-string field" in {
    assertErrors (expectedObject) (getObject ("""{"obj": 1}"""))
    assertErrors (expectedObject) (getObject ("""{"obj": ""}"""))
    assertErrors (expectedObject) (getObject ("""{"obj": []}"""))
  }}
