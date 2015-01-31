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

package example

import scala.util.Random

import com.fasterxml.jackson.databind.JsonNode
import com.jayway.restassured.RestAssured.given
import com.jayway.restassured.response.{Response => RestAssuredResponse}
import com.jayway.restassured.specification.ResponseSpecification
import com.treode.async.stubs.{StubGlobals, StubScheduler}, StubGlobals.scheduler
import com.treode.store.{Bytes, Cell, TxClock, TxId, WriteOp}, WriteOp._
import com.treode.store.stubs.StubStore
import com.treode.twitter.finagle.http.filter._
import com.twitter.finagle.Http
import org.hamcrest.{Description, Matcher, Matchers, TypeSafeMatcher}, Matchers._
import org.scalatest.FreeSpec

class ResourceSpec extends FreeSpec {

  def served (test: (Int, StubStore) => Any) {
    val store = StubStore()
    val port = Random.nextInt (65535 - 49152) + 49152
    val server = Http.serve (
      s":$port",
      NettyToFinagle andThen
      BadRequestFilter andThen
      JsonExceptionFilter andThen
      new Resource (0, store))
    try {
      test (port, store)
    } finally {
      server.close()
    }}

  def cell (key: String, time: TxClock): Cell =
    Cell (Bytes (key), time, None)

  def cell (key: String, time: TxClock, json: String): Cell =
    Cell (Bytes (key), time, Some (json.fromJson [JsonNode] .toBytes))

  def assertSeq [T] (xs: T*) (actual: Seq [T]): Unit =
    assertResult (xs) (actual)

  implicit class RichResposne (rsp: RestAssuredResponse) {

    def etag: TxClock = {
      val string = rsp.getHeader ("ETag")
      assert (string != null, "Expected response to have an ETag.")
      val parse = TxClock.parse (string)
      assert (parse.isDefined, s"""Could not parse ETag "$string" as a TxClock""")
      parse.get
    }}

  implicit class RichResponseSpecification (rsp: ResponseSpecification) {

    def etag (ts: TxClock): Unit =
      rsp.header ("ETag", ts.toString)
  }

  class JsonMatcher (expected: String) extends TypeSafeMatcher [String] {

    def matchesSafely (actual: String): Boolean =
      expected.fromJson [JsonNode] == actual.fromJson [JsonNode]

    def describeTo (desc: Description): Unit =
      desc.appendText (expected);
  }

  def matchesJson (expected: String): Matcher [String] =
    new JsonMatcher (expected)

  def update (store: StubStore, ct: TxClock, key: String, value: String): TxClock =
    store.write (
      TxId (Bytes (Random.nextInt), 0),
      ct,
      Update (123, Bytes (key), value.fromJson [JsonNode] .toBytes)
    ) .await

  "When the database is empty" - {

    "GET /table/123?key=abc should respond Not Found" in
      served { case (port, store) =>
        given
          .port (port)
        .expect
          .statusCode (404)
        .when
          .get ("/table/123?key=abc")
      }

    "PUT /table/123?key=abc should respond Ok with an etag" in
      served { case (port, store) =>
        val body = "\"i have been stored\""
        val rsp = given
          .port (port)
          .body (body)
        .expect
          .statusCode (200)
        .when
          .put ("/table/123?key=abc")
        val etag = rsp.etag
        assertSeq (cell ("abc", etag, body)) (store.scan (123))
      }

    "DELETE /table/123?key=abc should respond Ok with an etag" in
      served { case (port, store) =>
        val body = "\"i have been stored\""
        val rsp = given
          .port (port)
          .body (body)
        .expect
          .statusCode (200)
        .when
          .delete ("/table/123?key=abc")
        val etag = rsp.etag
        assertSeq (cell ("abc", etag)) (store.scan (123))
      }}

  "When the database has an entry" - {

    val entity = "\"you found me\""
    val entity2 = "\"i have been stored\""

    def addData (store: StubStore): TxClock =
      update (store, TxClock.MinValue, "abc", entity)

    "GET /table/123?key=abc should respond Ok" in
      served { case (port, store) =>
        val ts = addData (store)
        given
          .port (port)
        .expect
          .statusCode (200)
          .body (equalTo (entity))
        .when
          .get ("/table/123?key=abc")
      }

    "GET /table/123?key=abc with If-Modified-Since:0 should respond Ok" in
      served { case (port, store) =>
        val ts = addData (store)
        given
          .port (port)
          .header ("If-Modified-Since", "0")
        .expect
          .statusCode (200)
          .body (equalTo (entity))
        .when
          .get ("/table/123?key=abc")
      }

    "GET /table/123?key=abc with If-Modified-Since:1 should respond Not Modified" in
      served { case (port, store) =>
        val ts = addData (store)
        given
          .port (port)
          .header ("If-Modified-Since", "1")
        .expect
          .statusCode (304)
        .when
          .get ("/table/123?key=abc")
      }

    "GET /table/123?key=abc with Last-Modification-Before:0 should respond Not Found" in
      served { case (port, store) =>
        val ts = addData (store)
        given
          .port (port)
          .header ("Last-Modification-Before", "0")
        .expect
          .statusCode (404)
        .when
          .get ("/table/123?key=abc")
      }

    "GET /table/123?key=abc with Last-Modification-Before:1 should respond Ok" in
      served { case (port, store) =>
        val ts = addData (store)
        given
          .port (port)
          .header ("Last-Modification-Before", "1")
        .expect
          .statusCode (200)
          .body (equalTo (entity))
        .when
          .get ("/table/123?key=abc")
      }

    "PUT /table/123?key=abc should respond Ok with an etag" in
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .body (entity2)
        .expect
          .statusCode (200)
        .when
          .put ("/table/123?key=abc")
        val ts2 = rsp.etag
        assertSeq (cell ("abc", ts2, entity2), cell ("abc", ts1, entity)) (store.scan (123))
      }

    "PUT /table/123?key=abc with If-Unmodified-Since:1 should respond Ok with an etag" in
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .header ("If-Unmodified-Since", "1")
          .body (entity2)
        .expect
          .statusCode (200)
        .when
          .put ("/table/123?key=abc")
        val ts2 = rsp.etag
        assertSeq (cell ("abc", ts2, entity2), cell ("abc", ts1, entity)) (store.scan (123))
      }

    "PUT /table/123?key=abc with If-Unmodified-Since:0 should respond Precondition Failed" in
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .header ("If-Unmodified-Since", "0")
          .body (entity2)
        .expect
          .statusCode (412)
        .when
          .put ("/table/123?key=abc")
        assertSeq (cell ("abc", ts1, entity)) (store.scan (123))
      }

    "DELETE /table/123?key=abc should respond Ok with an etag" in
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
        .expect
          .statusCode (200)
        .when
          .delete ("/table/123?key=abc")
        val ts2 = rsp.etag
        assertSeq (cell ("abc", ts2), cell ("abc", ts1, entity)) (store.scan (123))
      }

    "DELETE /table/123?key=abc with If-Unmodified-Since:1 should respond Ok with an etag" in
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .header ("If-Unmodified-Since", "1")
        .expect
          .statusCode (200)
        .when
          .delete ("/table/123?key=abc")
        val ts2 = rsp.etag
        assertSeq (cell ("abc", ts2), cell ("abc", ts1, entity)) (store.scan (123))
      }

    "DELETE /table/123?key=abc with If-Unmodified-Since:0 should respond Precondition Failed" in
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .header ("If-Unmodified-Since", "0")
        .expect
          .statusCode (412)
        .when
          .delete ("/table/123?key=abc")
        assertSeq (cell ("abc", ts1, entity)) (store.scan (123))
      }}

  "When the database has entries and history" - {

    def addData (store: StubStore) {
      var t = update (store, TxClock.MinValue, "a", "\"a1\"")
      t = update (store, t, "a", "\"a2\"")
      t = update (store, t, "b", "\"b1\"")
      t = update (store, t, "b", "\"b2\"")
      t = update (store, t, "c", "\"c1\"")
      t = update (store, t, "c", "\"c2\"")
    }

    "GET /table/123 should work" in {
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .expect
          .statusCode (200)
          .body (matchesJson ("""[
            {"key": "a", "time": 2, "value": "a2"},
            {"key": "b", "time": 4, "value": "b2"},
            {"key": "c", "time": 6, "value": "c2"}
          ]"""))
        .when
          .get ("/table/123")
      }}

    "GET /table/123 with Last-Modification-Before:4 should work" in {
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .header ("Last-Modification-Before", "4")
        .expect
          .statusCode (200)
          .body (matchesJson ("""[
            {"key": "a", "time": 2, "value": "a2"},
            {"key": "b", "time": 4, "value": "b2"}
          ]"""))
        .when
          .get ("/table/123")
      }}

    "GET /table/123 with If-Modified-Since:3 should work" in {
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .header ("If-Modified-Since", "3")
        .expect
          .statusCode (200)
          .body (matchesJson ("""[
            {"key": "b", "time": 4, "value": "b2"},
            {"key": "c", "time": 6, "value": "c2"}
          ]"""))
        .when
          .get ("/table/123")
      }}

    "GET /table/123?slice=0&nslices=2 should work" in {
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .param ("slice", "0")
          .param ("nslices", "2")
        .expect
          .statusCode (200)
          .body (matchesJson ("""[
            {"key": "a", "time": 2, "value": "a2"}
          ]"""))
        .when
          .get ("/table/123")
      }}

    "GET /table/123?slice=1&nslices=2 should work" in {
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .param ("slice", "1")
          .param ("nslices", "2")
        .expect
          .statusCode (200)
          .body (matchesJson ("""[
            {"key": "b", "time": 4, "value": "b2"},
            {"key": "c", "time": 6, "value": "c2"}
          ]"""))
        .when
          .get ("/table/123")
      }}

    "GET /history/123 should work" in {
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
        .expect
          .statusCode (200)
          .body (matchesJson ("""[
            {"key": "a", "time": 2, "value": "a2"},
            {"key": "a", "time": 1, "value": "a1"},
            {"key": "b", "time": 4, "value": "b2"},
            {"key": "b", "time": 3, "value": "b1"},
            {"key": "c", "time": 6, "value": "c2"},
            {"key": "c", "time": 5, "value": "c1"}
          ]"""))
        .when
          .get ("/history/123")
      }}

    "GET /history/123 with Last-Modification-Before:3 should work" in {
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .header ("Last-Modification-Before", "3")
        .expect
          .statusCode (200)
          .body (matchesJson ("""[
            {"key": "a", "time": 2, "value": "a2"},
            {"key": "a", "time": 1, "value": "a1"},
            {"key": "b", "time": 3, "value": "b1"}
          ]"""))
        .when
          .get ("/history/123")
      }}

    "GET /history/123 with If-Modified-Since:3 should work" in {
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .header ("If-Modified-Since", "3")
        .expect
          .statusCode (200)
          .body (matchesJson ("""[
            {"key": "b", "time": 4, "value": "b2"},
            {"key": "c", "time": 6, "value": "c2"},
            {"key": "c", "time": 5, "value": "c1"}
          ]"""))
        .when
          .get ("/history/123")
      }}

    "GET /history/123?slice=0&nslices=2 should work" in {
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .param ("slice", "0")
          .param ("nslices", "2")
        .expect
          .statusCode (200)
          .body (matchesJson ("""[
            {"key": "a", "time": 2, "value": "a2"},
            {"key": "a", "time": 1, "value": "a1"}
          ]"""))
        .when
          .get ("/history/123")
      }}

    "GET /history/123?slice=1&nslices=2 should work" in {
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .param ("slice", "1")
          .param ("nslices", "2")
        .expect
          .statusCode (200)
          .body (matchesJson ("""[
            {"key": "b", "time": 4, "value": "b2"},
            {"key": "b", "time": 3, "value": "b1"},
            {"key": "c", "time": 6, "value": "c2"},
            {"key": "c", "time": 5, "value": "c1"}
          ]"""))
        .when
          .get ("/history/123")
      }}}

  "When the user is cantankerous" - {

    "and gives bad URIs" - {

      "GET /table should yield Not Found" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
          .expect
            .statusCode (404)
          .when
            .get ("/table")
        }}

      "GET /history should yield Not Found" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
          .expect
            .statusCode (404)
          .when
            .get ("/history")
        }}

      "PUT /table/123 should yield Method Not Allowed" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
          .expect
            .statusCode (405)
          .when
            .put ("/table/123")
        }}

      "DELETE /table/123 should yield Method Not Allowed" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
          .expect
            .statusCode (405)
          .when
            .delete ("/table/123")
        }}}

    "and gives bad clock values" - {

      "GET /table/123 with Last-Modification-Before:abc should yield Bad Request" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .header ("Last-Modification-Before", "abc")
          .expect
            .statusCode (400)
            .body (equalTo ("Bad time for Last-Modification-Before: abc"))
          .when
            .get ("/table/123")
        }}

      "GET /table/123 with If-Modified-Since:abc should yield Bad Request" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .header ("If-Modified-Since", "abc")
          .expect
            .statusCode (400)
            .body (equalTo ("Bad time for If-Modified-Since: abc"))
          .when
            .get ("/table/123")
        }}

      "DELETE /table/123?key=abc with If-Unmodified-Since:abc should yield Bad Request" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .param ("key", "abc")
            .header ("If-Unmodified-Since", "abc")
          .expect
            .statusCode (400)
            .body (equalTo ("Bad time for If-Unmodified-Since: abc"))
          .when
            .delete ("/table/123")
        }}}

    "and gives bad slice numbers" - {

      "GET /table/123?slice=abc&nslices=2 should yield Bad Request" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .param ("slice", "abc")
            .param ("nslices", "2")
          .expect
            .statusCode (400)
            .body (equalTo ("Bad integer for slice: abc"))
          .when
            .get ("/table/123")
        }}

      "GET /table/123?slice=0&nslices=abc should yield Bad Request" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .param ("slice", "2")
            .param ("nslices", "abc")
          .expect
            .statusCode (400)
            .body (equalTo ("Bad integer for nslices: abc"))
          .when
            .get ("/table/123")
        }}

      "GET /table/123?slice=0 should yield Bad Request" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .param ("slice", "0")
          .expect
            .statusCode (400)
            .body (equalTo ("Both slice and nslices are needed together"))
          .when
            .get ("/table/123")
        }}

      "GET /table/123?nslices=0 should yield Bad Request" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .param ("slice", "0")
          .expect
            .statusCode (400)
            .body (equalTo ("Both slice and nslices are needed together"))
          .when
            .get ("/table/123")
        }}

      "GET /table/123?slice=0&nslices=5 should yield Bad Request" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .param ("slice", "0")
            .param ("nslices", "5")
          .expect
            .statusCode (400)
            .body (equalTo ("Number of slices must be a power of two and at least one."))
          .when
            .get ("/table/123")
        }}

      "GET /table/123?slice=2&nslices=2 should yield Bad Request" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .param ("slice", "2")
            .param ("nslices", "2")
          .expect
            .statusCode (400)
            .body (equalTo ("The slice must be between 0 (inclusive) and the number of slices (exclusive)."))
          .when
            .get ("/table/123")
        }}}}}