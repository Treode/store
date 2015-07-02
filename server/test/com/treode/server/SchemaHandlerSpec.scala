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

package com.treode.server

import scala.util.Random

import com.jayway.restassured.RestAssured.given
import com.treode.twitter.finagle.http.filter.NettyToFinagle
import com.twitter.finagle.Http
import com.twitter.finagle.http.filter.ExceptionFilter
import org.hamcrest.Matchers, Matchers._
import org.scalamock.scalatest.MockFactory
import org.scalatest.FlatSpec

class SchemaHandlerSpec extends FlatSpec with MockFactory {

  def served (test: (Int, Librarian) => Any) {
    val librarian = mock [Librarian]
    val port = Random.nextInt (65535 - 49152) + 49152
    val server = Http.serve (
      s":$port",
      NettyToFinagle andThen
      ExceptionFilter andThen
      new SchemaHandler (librarian))
    try {
      test (port, librarian)
    } finally {
      server.close()
    }}

  val SchemaParser.CompilerSuccess (schema) =
    SchemaParser.parse ("table table { id: 0x1; };")

  "The SchemaHandler" should "handle GET" in
    served { case (port, librarian) =>
      (librarian.schema _) .expects() .returning (schema)
      given
        .port (port)
      .expect
        .statusCode (200)
        .body (equalTo ("table table { id: 0x1; };"))
      .when
        .get ("/")
    }

  it should "handle PUT" in
    served { case (port, librarian) =>
      (librarian.schema_= _) .expects (schema) .returning (())
      given
        .port (port)
        .body ("table table { id: 0x1; };")
      .expect
        .statusCode (200)
      .when
        .put ("/")
    }

  it should "handle error on PUT" in
    served { case (port, librarian) =>
      given
        .port (port)
        .body ("garbage")
      .expect
        .statusCode (400)
        .body (containsString ("Bad definition of clause"))
      .when
        .put ("/")
    }

  it should "reject other methods" in
    served { case (port, librarian) =>
      given
        .port (port)
      .expect
        .statusCode (405)
      .when
        .post ("/")
    }}
