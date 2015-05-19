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

  val expectedFrom0to2 = ExpectedNumber ("", 0, 2)
  val expectedFieldFrom0to2 = ExpectedNumber ("n", 0, 2)
  val expectedObject = ExpectedObject ("obj")
  val expectedPath = ExpectedFilePath ("")
  val expectedPathField = ExpectedFilePath ("p")
  val path = Paths.get ("/a/b/c")

  def getFilePath (s: String) =
    JsonReader (s) .getFilePath()

  def getFilePathField (s: String) =
    JsonReader (s) .getFilePath ("p")

  def getInt (s: String, lower: Int = Int.MinValue, upper: Int = Int.MaxValue) =
    JsonReader (s) .getInt (lower, upper)

  def getIntField (s: String, lower: Int = Int.MinValue, upper: Int = Int.MaxValue) =
    JsonReader (s) .getInt ("n", lower, upper)

  def getLong (s: String, lower: Long = Long.MinValue, upper: Long = Long.MaxValue) =
    JsonReader (s) .getLong (lower, upper)

  def getLongField (s: String, lower: Long = Long.MinValue, upper: Long = Long.MaxValue) =
    JsonReader (s) .getLong ("n", lower, upper)

  def readObjectField (s: String, lower: Int = Int.MinValue, upper: Int = Int.MaxValue) =
    JsonReader (s) .readObject ("obj") (_.getInt ("n", lower, upper))

  def readArray (s: String, lower: Int = Int.MinValue, upper: Int = Int.MaxValue) =
    JsonReader (s) .readArray (_.getInt (lower, upper))

  def readArrayField (s: String, lower: Int = Int.MinValue, upper: Int = Int.MaxValue) =
    JsonReader (s) .readArray ("arr") (_.getInt (lower, upper))

  def assertErrors [A] (expected: Message*) (actual: Notification [A]): Unit =
    assertResult (Notification.Errors (expected)) (actual)

  "getFilePath" should "read a valid path" in {
    assertResult (path) (getFilePath (""" "/a/b/c" """) .get)
  }

  it should "report a non-string" in {
    assertErrors (expectedPath) (getFilePath ("""1"""))
    assertErrors (expectedPath) (getFilePath ("""[]"""))
    assertErrors (expectedPath) (getFilePath ("""{}"""))
  }

  "getFilePath from field" should "read a valid path" in {
    assertResult (path) (getFilePathField ("""{"p": "/a/b/c"}""") .get)
  }

  it should "report a missing field" in {
    assertErrors (expectedPathField) (getFilePathField ("""{}"""))
  }

  it should "report a non-string field" in {
    assertErrors (expectedPathField) (getFilePathField ("""{"p": 1}"""))
    assertErrors (expectedPathField) (getFilePathField ("""{"p": []}"""))
    assertErrors (expectedPathField) (getFilePathField ("""{"p": {}}"""))
  }

  "getInt" should "read a valid int" in {
    assertResult (0) (getInt ("0") .get)
    assertResult (Int.MinValue) (getInt ("-2147483648") .get)
    assertResult (Int.MaxValue) (getInt ("2147483647") .get)
  }

  it should "report a non-integer field" in {
    assertErrors (expectedFrom0to2) (getInt (""" "hello" """, 0, 2))
    assertErrors (expectedFrom0to2) (getInt ("[]", 0, 2))
    assertErrors (expectedFrom0to2) (getInt ("{}", 0, 2))
  }

  it should "report a value out of range" in {
    assertErrors (expectedFrom0to2) (getInt ("-1", 0, 2))
    assertErrors (expectedFrom0to2) (getInt ("3", 0, 2))
  }

  "getInt from field" should "read a valid int field" in {
    assertResult (0) (getIntField ("""{"n": 0}""") .get)
    assertResult (Int.MinValue) (getIntField ("""{"n": -2147483648}""") .get)
    assertResult (Int.MaxValue) (getIntField ("""{"n": 2147483647}""") .get)
  }

  it should "report a missing field" in {
    assertErrors (expectedFieldFrom0to2) (getIntField ("""{}""", 0, 2))
  }

  it should "report a non-integer field" in {
    assertErrors (expectedFieldFrom0to2) (getIntField ("""{"n": "hello"}""", 0, 2))
    assertErrors (expectedFieldFrom0to2) (getIntField ("""{"n": "[]"}""", 0, 2))
    assertErrors (expectedFieldFrom0to2) (getIntField ("""{"n": "{}"}""", 0, 2))
  }

  it should "report a value out of range" in {
    assertErrors (expectedFieldFrom0to2) (getIntField ("""{"n": -1}""", 0, 2))
    assertErrors (expectedFieldFrom0to2) (getIntField ("""{"n": 3}""", 0, 2))
  }

  "getLong" should "read a valid long" in {
    assertResult (0) (getInt ("0") .get)
    assertResult (Int.MinValue) (getLong ("-2147483648") .get)
    assertResult (Int.MaxValue) (getLong ("2147483647") .get)
    assertResult (Long.MinValue) (getLong ("-9223372036854775808") .get)
    assertResult (Long.MaxValue) (getLong ("9223372036854775807") .get)
  }

  it should "report a non-integer field" in {
    assertErrors (expectedFrom0to2) (getLong (""" "hello" """, 0, 2))
    assertErrors (expectedFrom0to2) (getLong ("[]", 0, 2))
    assertErrors (expectedFrom0to2) (getLong ("{}", 0, 2))
  }

  it should "report a value out of range" in {
    assertErrors (expectedFrom0to2) (getLong ("-1", 0, 2))
    assertErrors (expectedFrom0to2) (getLong ("3", 0, 2))
  }

  "getLong from field" should "read an valid long field" in {
    assertResult (0) (getLongField ("""{"n": 0}""") .get)
    assertResult (Int.MinValue) (getLongField ("""{"n": -2147483648}""") .get)
    assertResult (Int.MaxValue) (getLongField ("""{"n": 2147483647}""") .get)
    assertResult (Long.MinValue) (getLongField ("""{"n": -9223372036854775808}""") .get)
    assertResult (Long.MaxValue) (getLongField ("""{"n": 9223372036854775807}""") .get)
  }

  it should "report a missing field" in {
    assertErrors (expectedFieldFrom0to2) (getLongField ("""{}""", 0, 2))
  }

  it should "report a non-integer field" in {
    assertErrors (expectedFieldFrom0to2) (getLongField ("""{"n": "hello"}""", 0, 2))
    assertErrors (expectedFieldFrom0to2) (getLongField ("""{"n": "[]"}""", 0, 2))
    assertErrors (expectedFieldFrom0to2) (getLongField ("""{"n": "{}"}""", 0, 2))
  }

  it should "report a value out of range" in {
    assertErrors (expectedFieldFrom0to2) (getLongField ("""{"n": -1}""", 0, 2))
    assertErrors (expectedFieldFrom0to2) (getLongField ("""{"n": 3}""", 0, 2))
  }

  "readObject from field" should "read a valid object" in {
    assertResult (1) (readObjectField ("""{"obj": {"n": 1}}""") .get)
  }

  it should "report a missing field" in {
    assertErrors (ExpectedObject ("obj")) (readObjectField ("""{}"""))
  }

  it should "report a non-string field" in {
    assertErrors (expectedObject) (readObjectField ("""{"obj": 1}"""))
    assertErrors (expectedObject) (readObjectField ("""{"obj": ""}"""))
    assertErrors (expectedObject) (readObjectField ("""{"obj": []}"""))
  }

  "readArray" should "read a valid array" in {
    assertResult (Seq (1, 2, 3)) (readArray ("""[1, 2, 3]""") .get)
    assertResult (Seq (1)) (readArray ("""[1]""") .get)
    assertResult (Seq.empty) (readArray ("""[]""") .get)
  }

  it should "report multiple errors" in {
    assertErrors (ExpectedNumber ("1", 1, 3), ExpectedNumber ("3", 1, 3)) {
      readArray ("""[1, 4, 2, 5]""", 1, 3)
    }}

  "readArray from field" should "read a valid array" in {
    assertResult (Seq (1, 2, 3)) (readArrayField ("""{"arr": [1, 2, 3]}""") .get)
    assertResult (Seq (1)) (readArrayField ("""{"arr": [1]}""") .get)
    assertResult (Seq.empty) (readArrayField ("""{"arr": []}""") .get)
  }

  it should "yield empty for a missing field" in {
    assertResult (Seq.empty) (readArrayField ("""{}""") .get)
  }

  it should "report multiple errors" in {
    assertErrors (ExpectedNumber ("arr/1", 1, 3), ExpectedNumber ("arr/3", 1, 3)) {
      readArrayField ("""{"arr": [1, 4, 2, 5]}""", 1, 3)
    }}}
