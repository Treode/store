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

import com.fasterxml.jackson.databind.JsonNode
import com.jayway.restassured.RestAssured.given
import com.jayway.restassured.response.{Response => RestAssuredResponse}
import com.jayway.restassured.specification.ResponseSpecification
import com.treode.async.stubs.{StubGlobals, StubScheduler}, StubGlobals.scheduler
import com.treode.store.{Bytes, Cell, TxClock, WriteOp, StaleException, TxId, Key}
import com.treode.store.stubs.StubStore
import com.treode.twitter.finagle.http.filter._
import com.twitter.finagle.Http
import org.hamcrest.{Description, Matcher, Matchers, TypeSafeMatcher}, Matchers._
import org.scalatest.FreeSpec
import scala.collection.mutable.HashMap

class ResourceSpec extends FreeSpec {

  def served (test: (Int, SchematicStubStore) => Any) {
    val store = StubStore ()
    val port = Random.nextInt (65535 - 49152) + 49152
	val map = new HashMap [String, Long]();
	map += ("table1" -> 0x1);
	map += ("table2" -> 0x2);
	map += ("table3" -> 0x3);
	map += ("table4" -> 0x4);
    val schematicStore = new SchematicStubStore (store, new Schema (map))
    val server = Http.serve (
      s":$port",
      NettyToFinagle andThen
      BadRequestFilter andThen
      JsonExceptionFilter andThen
      new Resource (0, schematicStore))
    try {
      test (port, schematicStore)
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

	def valueTxClock: TxClock = {
      val string = rsp.getHeader ("Value-TxClock")
      assert (string != null, "Expected response to have a Value-TxClock.")
      val parse = TxClock.parse (string)
      assert (parse.isDefined, s"""Could not parse Value-TxClock "$string" as a TxClock""")
      parse.get
    }}

  implicit class RichResponseSpecification (rsp: ResponseSpecification) {

    def valueTxClock (ts: TxClock): Unit =
      rsp.header ("Value-TxClock", ts.toString)
  }

  class JsonMatcher (expected: String) extends TypeSafeMatcher [String] {

    def matchesSafely (actual: String): Boolean =
      expected.fromJson [JsonNode] == actual.fromJson [JsonNode]

    def describeTo (desc: Description): Unit =
      desc.appendText (expected);
  }

  def matchesJson (expected: String): Matcher [String] =
    new JsonMatcher (expected)

  def update (store: SchematicStubStore, ct: TxClock, key: String, value: String): TxClock =
    store.update (
      "table1",
      key,
      value.fromJson [JsonNode],
      TxId (Bytes (Random.nextInt), 0),
      ct
    ) .await

  "When the database is empty" - {

    "GET /table/table1?key=abc should respond Not Found" in
      served { case (port, store) =>
        given
          .port (port)
        .expect
          .statusCode (404)
        .when
          .get ("/table/table1?key=abc")
      }

    "PUT /table/table1?key=abc should respond Ok with a valueTxClock" in
      served { case (port, store) =>
        val body = "\"i have been stored\""
        val rsp = given
          .port (port)
          .body (body)
        .expect
          .statusCode (200)
        .when
          .put ("/table/table1?key=abc")
        val valueTxClock = rsp.valueTxClock
        assertSeq (cell ("abc", valueTxClock, body)) (store.scan ("table1"))
      }

    "DELETE /table/table1?key=abc should respond Ok with a valueTxClock" in
      served { case (port, store) =>
        val body = "\"i have been stored\""
        val rsp = given
          .port (port)
          .body (body)
        .expect
          .statusCode (200)
        .when
          .delete ("/table/table1?key=abc")
        val valueTxClock = rsp.valueTxClock
        assertSeq (cell ("abc", valueTxClock)) (store.scan ("table1"))
      }}

  "When the database has an entry" - {

    val entity = "\"you found me\""
    val entity2 = "\"i have been stored\""

    def addData (store: SchematicStubStore): TxClock =
      update (store, TxClock.MinValue, "abc", entity)

    "GET /table/table1?key=abc should respond Ok" in
      served { case (port, store) =>
        val ts = addData (store)
        given
          .port (port)
        .expect
          .statusCode (200)
          .body (equalTo (entity))
        .when
          .get ("/table/table1?key=abc")
      }

    "GET /table/table1?key=abc with Condition-TxClock:0 should respond Ok" in
      served { case (port, store) =>
        val ts = addData (store)
        given
          .port (port)
          .header ("Condition-TxClock", "0")
        .expect
          .statusCode (200)
          .body (equalTo (entity))
        .when
          .get ("/table/table1?key=abc")
      }

    "GET /table/table1?key=abc with Condition-TxClock:1 should respond Not Modified" in
      served { case (port, store) =>
        val ts = addData (store)
        given
          .port (port)
          .header ("Condition-TxClock", "1")
        .expect
          .statusCode (304)
        .when
          .get ("/table/table1?key=abc")
      }

    "GET /table/table1?key=abc with Request-TxClock:0 should respond Not Found" in
      served { case (port, store) =>
        val ts = addData (store)
        given
          .port (port)
          .header ("Request-TxClock", "0")
        .expect
          .statusCode (404)
        .when
          .get ("/table/table1?key=abc")
      }

    "GET /table/table1?key=abc with Request-TxClock:1 should respond Ok" in
      served { case (port, store) =>
        val ts = addData (store)
        given
          .port (port)
          .header ("Request-TxClock", "1")
        .expect
          .statusCode (200)
          .body (equalTo (entity))
        .when
          .get ("/table/table1?key=abc")
      }

    "PUT /table/table1?key=abc should respond Ok with a valueTxClock" in
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .body (entity2)
        .expect
          .statusCode (200)
        .when
          .put ("/table/table1?key=abc")
        val ts2 = rsp.valueTxClock
        assertSeq (cell ("abc", ts2, entity2), cell ("abc", ts1, entity)) (store.scan ("table1"))
      }

    "PUT /table/table1?key=abc with If-Unmodified-Since:1 should respond Ok with a valueTxClock" in
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .header ("Condition-TxClock", "1")
          .body (entity2)
        .expect
          .statusCode (200)
        .when
          .put ("/table/table1?key=abc")
        val ts2 = rsp.valueTxClock
        assertSeq (cell ("abc", ts2, entity2), cell ("abc", ts1, entity)) (store.scan ("table1"))
      }

    "PUT /table/table1?key=abc with If-Unmodified-Since:0 should respond Precondition Failed" in
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .header ("Condition-TxClock", "0")
          .body (entity2)
        .expect
          .statusCode (412)
        .when
          .put ("/table/table1?key=abc")
        assertSeq (cell ("abc", ts1, entity)) (store.scan ("table1"))
      }

    "DELETE /table/table1?key=abc should respond Ok with a valueTxClock" in
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
        .expect
          .statusCode (200)
        .when
          .delete ("/table/table1?key=abc")
        val ts2 = rsp.valueTxClock
        assertSeq (cell ("abc", ts2), cell ("abc", ts1, entity)) (store.scan ("table1"))
      }

    "DELETE /table/table1?key=abc with If-Unmodified-Since:1 should respond Ok with a valueTxClock" in
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .header ("Condition-TxClock", "1")
        .expect
          .statusCode (200)
        .when
          .delete ("/table/table1?key=abc")
        val ts2 = rsp.valueTxClock
        assertSeq (cell ("abc", ts2), cell ("abc", ts1, entity)) (store.scan ("table1"))
      }

    "DELETE /table/table1?key=abc with If-Unmodified-Since:0 should respond Precondition Failed" in
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .header ("Condition-TxClock", "0")
        .expect
          .statusCode (412)
        .when
          .delete ("/table/table1?key=abc")
        assertSeq (cell ("abc", ts1, entity)) (store.scan ("table1"))
      }}

  "When the database has entries and history" - {

    def addData (store: SchematicStubStore) {
      var t = update (store, TxClock.MinValue, "a", "\"a1\"")
      t = update (store, t, "a", "\"a2\"")
      t = update (store, t, "b", "\"b1\"")
      t = update (store, t, "b", "\"b2\"")
      t = update (store, t, "c", "\"c1\"")
      t = update (store, t, "c", "\"c2\"")
    }

    "GET /table/table1 should work" in {
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
          .get ("/table/table1")
      }}

    "GET /table/table1 with Request-TxClock:4 should work" in {
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .header ("Request-TxClock", "4")
        .expect
          .statusCode (200)
          .body (matchesJson ("""[
            {"key": "a", "time": 2, "value": "a2"},
            {"key": "b", "time": 4, "value": "b2"}
          ]"""))
        .when
          .get ("/table/table1")
      }}

    "GET /table/table1 with Condition-TxClock:3 should work" in {
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .header ("Condition-TxClock", "3")
        .expect
          .statusCode (200)
          .body (matchesJson ("""[
            {"key": "b", "time": 4, "value": "b2"},
            {"key": "c", "time": 6, "value": "c2"}
          ]"""))
        .when
          .get ("/table/table1")
      }}

    "GET /table/table1?slice=0&nslices=2 should work" in {
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
          .get ("/table/table1")
      }}

    "GET /table/table1?slice=1&nslices=2 should work" in {
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
          .get ("/table/table1")
      }}

    "GET /history/table1 should work" in {
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
          .get ("/history/table1")
      }}

    "GET /history/table1 with Request-TxClock:3 should work" in {
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .header ("Request-TxClock", "3")
        .expect
          .statusCode (200)
          .body (matchesJson ("""[
            {"key": "a", "time": 2, "value": "a2"},
            {"key": "a", "time": 1, "value": "a1"},
            {"key": "b", "time": 3, "value": "b1"}
          ]"""))
        .when
          .get ("/history/table1")
      }}

    "GET /history/table1 with Condition-TxClock:3 should work" in {
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .header ("Condition-TxClock", "3")
        .expect
          .statusCode (200)
          .body (matchesJson ("""[
            {"key": "b", "time": 4, "value": "b2"},
            {"key": "c", "time": 6, "value": "c2"},
            {"key": "c", "time": 5, "value": "c1"}
          ]"""))
        .when
          .get ("/history/table1")
      }}

    "GET /history/table1?slice=0&nslices=2 should work" in {
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
          .get ("/history/table1")
      }}

    "GET /history/table1?slice=1&nslices=2 should work" in {
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
          .get ("/history/table1")
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

      "PUT /table/table1 should yield Method Not Allowed" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
          .expect
            .statusCode (405)
          .when
            .put ("/table/table1")
        }}

      "DELETE /table/table1 should yield Method Not Allowed" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
          .expect
            .statusCode (405)
          .when
            .delete ("/table/table1")
        }}}

    "and gives bad clock values" - {

      "GET /table/table1 with Request-TxClock:abc should yield Bad Request" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .header ("Request-TxClock", "abc")
          .expect
            .statusCode (400)
            .body (equalTo ("Bad time for Request-TxClock: abc"))
          .when
            .get ("/table/table1")
        }}

      "GET /table/table1 with Condition-TxClock:abc should yield Bad Request" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .header ("Condition-TxClock", "abc")
          .expect
            .statusCode (400)
            .body (equalTo ("Bad time for Condition-TxClock: abc"))
          .when
            .get ("/table/table1")
        }}

      "DELETE /table/table1?key=abc with If-Unmodified-Since:abc should yield Bad Request" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .param ("key", "abc")
            .header ("Condition-TxClock", "abc")
          .expect
            .statusCode (400)
            .body (equalTo ("Bad time for Condition-TxClock: abc"))
          .when
            .delete ("/table/table1")
        }}}

    "and gives bad slice numbers" - {

      "GET /table/table1?slice=abc&nslices=2 should yield Bad Request" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .param ("slice", "abc")
            .param ("nslices", "2")
          .expect
            .statusCode (400)
            .body (equalTo ("Bad integer for slice: abc"))
          .when
            .get ("/table/table1")
        }}

      "GET /table/table1?slice=0&nslices=abc should yield Bad Request" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .param ("slice", "2")
            .param ("nslices", "abc")
          .expect
            .statusCode (400)
            .body (equalTo ("Bad integer for nslices: abc"))
          .when
            .get ("/table/table1")
        }}

      "GET /table/table1?slice=0 should yield Bad Request" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .param ("slice", "0")
          .expect
            .statusCode (400)
            .body (equalTo ("Both slice and nslices are needed together"))
          .when
            .get ("/table/table1")
        }}

      "GET /table/table1?nslices=0 should yield Bad Request" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .param ("slice", "0")
          .expect
            .statusCode (400)
            .body (equalTo ("Both slice and nslices are needed together"))
          .when
            .get ("/table/table1")
        }}

      "GET /table/table1?slice=0&nslices=5 should yield Bad Request" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .param ("slice", "0")
            .param ("nslices", "5")
          .expect
            .statusCode (400)
            .body (equalTo ("Number of slices must be a power of two and at least one."))
          .when
            .get ("/table/table1")
        }}

      "GET /table/table1?slice=2&nslices=2 should yield Bad Request" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .param ("slice", "2")
            .param ("nslices", "2")
          .expect
            .statusCode (400)
            .body (equalTo ("The slice must be between 0 (inclusive) and the number of slices (exclusive)."))
          .when
            .get ("/table/table1")
        }}}}}
