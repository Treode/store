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

package com.treode.twitter.server.handler

import scala.util.Random

import com.fasterxml.jackson.databind.JsonNode
import com.treode.store.{Store, StoreController}
import com.treode.twitter.finagle.http.mapper
import com.treode.twitter.finagle.http.filter._
import com.twitter.finagle.{Http, Service}
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.http.filter.ExceptionFilter
import org.hamcrest.{Description, Matcher, Matchers, TypeSafeMatcher}
import org.scalamock.scalatest.MockFactory
import org.scalatest.Suite

trait SpecTools extends MockFactory {
  this: Suite =>

  def handler (controller: StoreController): Service [Request, Response]

  def served (test: (Int, StoreController) => Any) {
    val controller = mock [StoreController]
    val port = Random.nextInt (65535 - 49152) + 49152
    val server = Http.serve (
      s":$port",
      NettyToFinagle andThen
      ExceptionFilter andThen
      JsonExceptionFilter andThen
      handler (controller))
    try {
      test (port, controller)
    } finally {
      server.close()
    }}

  class JsonMatcher (expected: String) extends TypeSafeMatcher [String] {

    def matchesSafely (actual: String): Boolean =
      mapper.readValue [JsonNode] (expected) == mapper.readValue [JsonNode] (actual)

    def describeTo (desc: Description): Unit =
      desc.appendText (expected);
  }

  def matchesJson (expected: String): Matcher [String] =
    new JsonMatcher (expected)
}
