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

package movies

import com.fasterxml.jackson.databind.JsonNode
import com.treode.store.TxClock
import com.twitter.finatra.test.MockResult
import org.scalatest.Suite
import org.scalatest.matchers.{Matcher, MatchResult}

trait ResourceSpecTools extends SpecTools {
  this: Suite =>

  class JsonMatcher (expected: String) extends Matcher [String] {

    def apply (actual: String): MatchResult =
      MatchResult (
        expected.fromJson [JsonNode] == actual.fromJson [JsonNode],
        s"$actual was not $expected",
        s"$actual was $expected")
  }

  def matchJson (expected: String) = new JsonMatcher (expected)

  implicit class RichMockResult (result: MockResult) {

    def etag: TxClock = {
      val string = result.getHeaders.get (ETag)
      assert (string.isDefined, "Expected response to have an ETag.")
      val parse = TxClock.parse (string.get)
      assert (parse.isDefined, s"""Could not parse ETag "${string.get}" as a TxClock""")
      parse.get
    }}}
