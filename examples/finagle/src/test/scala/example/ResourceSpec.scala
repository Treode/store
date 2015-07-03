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
import com.jayway.restassured.specification.{RequestSpecification, ResponseSpecification}
import com.treode.async.stubs.{StubGlobals, StubScheduler}, StubGlobals.scheduler
import com.treode.store.{Bytes, Cell, TxClock, TxId, WriteOp}, WriteOp._
import com.treode.store.stubs.StubStore
import com.treode.twitter.finagle.http.filter._
import com.twitter.finagle.Http
import org.hamcrest.{Description, Matcher, Matchers, TypeSafeMatcher}, Matchers._
import org.scalatest.FreeSpec

import Resource.{Row, Table, Unmatched, route}

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

  implicit class RichResponse (rsp: RestAssuredResponse) {

	  def valueTxClock: TxClock = {
      val string = rsp.getHeader ("Value-TxClock")
      assert (string != null, "Expected response to have a Value-TxClock.")
      val parse = TxClock.parse (string)
      assert (parse.isDefined, s"""Could not parse Value-TxClock "$string" as a TxClock""")
      parse.get
    }}

  implicit class RichRequestSpec (req: RequestSpecification) {

    def conditionTxClock (time: TxClock): RequestSpecification =
      req.header ("Condition-TxClock", time.time.toString)

    def readTxClock (time: TxClock): RequestSpecification =
      req.header ("Read-TxClock", time.time.toString)
  }

  implicit class RichResponseSpec (rsp: ResponseSpecification) {

    def valueTxClock (time: TxClock): ResponseSpecification =
      rsp.header ("Value-TxClock", time.time.toString)
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
      TxId (Bytes (Random.nextInt)),
      ct,
      Update (123, Bytes (key), value.fromJson [JsonNode] .toBytes)
    ) .await

  "Resource.route" - {
    "should route properly" in {
      assertResult (Table (123)) (route ("/123"))
      assertResult (Table (123)) (route ("/123/"))
      assertResult (Table (123)) (route ("/0173"))
      assertResult (Table (123)) (route ("/0173/"))
      assertResult (Table (123)) (route ("/0x7B"))
      assertResult (Table (123)) (route ("/0x7B/"))
      assertResult (Table (123)) (route ("/#7B"))
      assertResult (Table (123)) (route ("/#7B/"))
      assertResult (Row (123, "key")) (route ("/123/key"))
      assertResult (Row (123, "key")) (route ("/0173/key"))
      assertResult (Row (123, "key")) (route ("/0x7B/key"))
      assertResult (Row (123, "key")) (route ("/#7B/key"))
    }}

  "When the database is empty" - {

    "GET /123/abc should respond Not Found" in
      served { case (port, store) =>
        given
          .port (port)
        .expect
          .statusCode (404)
        .when
          .get ("/123/abc")
      }

    "PUT /123/abc should respond Ok" in
      served { case (port, store) =>
        val body = "\"i have been stored\""
        val rsp = given
          .port (port)
          .body (body)
        .expect
          .statusCode (200)
        .when
          .put ("/123/abc")
        val ts = rsp.valueTxClock
        assertSeq (cell ("abc", ts, body)) (store.scan (123))
      }

    "DELETE /123/abc should respond Ok" in
      served { case (port, store) =>
        val body = "\"i have been stored\""
        val rsp = given
          .port (port)
          .body (body)
        .expect
          .statusCode (200)
        .when
          .delete ("/123/abc")
        val ts = rsp.valueTxClock
        assertSeq (cell ("abc", ts)) (store.scan (123))
      }}

  "When the database has an entry" - {

    val entity = "\"you found me\""
    val entity2 = "\"i have been stored\""

    def addData (store: StubStore): TxClock =
      update (store, TxClock.MinValue, "abc", entity)

    "GET /123/abc should respond Ok" in
      served { case (port, store) =>
        val ts = addData (store)
        given
          .port (port)
        .expect
          .statusCode (200)
          .body (equalTo (entity))
        .when
          .get ("/123/abc")
      }

    "GET /123/abc with Condition-TxClock:0 should respond Ok" in
      served { case (port, store) =>
        val ts = addData (store)
        given
          .port (port)
          .conditionTxClock (ts - 1)
        .expect
          .statusCode (200)
          .body (equalTo (entity))
        .when
          .get ("/123/abc")
      }

    "GET /123/abc with Condition-TxClock:1 should respond Not Modified" in
      served { case (port, store) =>
        val ts = addData (store)
        given
          .port (port)
          .conditionTxClock (ts)
        .expect
          .statusCode (304)
        .when
          .get ("/123/abc")
      }

    "GET /123/abc with Read-TxClock:0 should respond Not Found" in
      served { case (port, store) =>
        val ts = addData (store)
        given
          .port (port)
          .readTxClock (ts - 1)
        .expect
          .statusCode (404)
        .when
          .get ("/123/abc")
      }

    "GET /123/abc with Read-TxClock:1 should respond Ok" in
      served { case (port, store) =>
        val ts = addData (store)
        given
          .port (port)
          .readTxClock (ts)
        .expect
          .statusCode (200)
          .body (equalTo (entity))
        .when
          .get ("/123/abc")
      }

    "PUT /123/abc should respond Ok" in
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .body (entity2)
        .expect
          .statusCode (200)
        .when
          .put ("/123/abc")
        val ts2 = rsp.valueTxClock
        assertSeq (cell ("abc", ts2, entity2), cell ("abc", ts1, entity)) (store.scan (123))
      }

    "PUT /123/abc with Condition-TxClock:1 should respond Ok" in
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .conditionTxClock (ts1)
          .body (entity2)
        .expect
          .statusCode (200)
        .when
          .put ("/123/abc")
        val ts2 = rsp.valueTxClock
        assertSeq (cell ("abc", ts2, entity2), cell ("abc", ts1, entity)) (store.scan (123))
      }

    "PUT /123/abc with Condition-TxClock:0 should respond Precondition Failed" in
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .conditionTxClock (ts1 - 1)
          .body (entity2)
        .expect
          .statusCode (412)
          .valueTxClock (ts1)
        .when
          .put ("/123/abc")
        assertSeq (cell ("abc", ts1, entity)) (store.scan (123))
      }

    "DELETE /123/abc should respond Ok" in
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
        .expect
          .statusCode (200)
        .when
          .delete ("/123/abc")
        val ts2 = rsp.valueTxClock
        assertSeq (cell ("abc", ts2), cell ("abc", ts1, entity)) (store.scan (123))
      }

    "DELETE /123/abc with Condition-TxClock:1 should respond Ok" in
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .conditionTxClock (ts1)
        .expect
          .statusCode (200)
        .when
          .delete ("/123/abc")
        val ts2 = rsp.valueTxClock
        assertSeq (cell ("abc", ts2), cell ("abc", ts1, entity)) (store.scan (123))
      }

    "DELETE /123/abc with Condition-TxClock:0 should respond Precondition Failed" in
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .conditionTxClock (ts1 - 1)
        .expect
          .statusCode (412)
          .valueTxClock (ts1)
        .when
          .delete ("/123/abc")
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

    "GET /123 should work" in {
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
          .get ("/123")
      }}

    "GET /table/123?until=4 should work" in {
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .param ("until", "4")
        .expect
          .statusCode (200)
          .body (matchesJson ("""[
            {"key": "a", "time": 2, "value": "a2"},
            {"key": "b", "time": 4, "value": "b2"}
          ]"""))
        .when
          .get ("/123")
      }}

    "GET /123?slice=0&nslices=2 should work" in {
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
          .get ("/123")
      }}}

  "When the user is cantankerous" - {

    "and gives bad URIs" - {

      "GET / should yield Not Found" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
          .expect
            .statusCode (404)
          .when
            .get ("/")
        }}

      "PUT /123 should yield Method Not Allowed" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
          .expect
            .statusCode (405)
          .when
            .put ("/123")
        }}

      "DELETE /123 should yield Method Not Allowed" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
          .expect
            .statusCode (405)
          .when
            .delete ("/123")
        }}}

    "and gives bad clock values" - {

      "GET /123 with Read-TxClock:abc should yield Bad Request" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .header ("Read-TxClock", "abc")
          .expect
            .statusCode (400)
            .body (equalTo ("Bad time for Read-TxClock: abc"))
          .when
            .get ("/123")
        }}

      "GET /123 with Condition-TxClock:abc should yield Bad Request" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .header ("Condition-TxClock", "abc")
          .expect
            .statusCode (400)
            .body (equalTo ("Bad time for Condition-TxClock: abc"))
          .when
            .get ("/123")
        }}

      "DELETE /123/abc with Condition-TxClock:abc should yield Bad Request" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .header ("Condition-TxClock", "abc")
          .expect
            .statusCode (400)
            .body (equalTo ("Bad time for Condition-TxClock: abc"))
          .when
            .delete ("/123/abc")
        }}}

    "and gives a bad slice" - {

      "GET /123?slice=abc&nslices=2 should yield Bad Request" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .param ("slice", "abc")
            .param ("nslices", "2")
          .expect
            .statusCode (400)
            .body (equalTo ("Bad integer for slice: abc"))
          .when
            .get ("/123")
        }}}

    "and gives a bad window" - {

      "GET /table/123?pick=foobar should yield Bad Request" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .param ("pick", "foobar")
          .expect
            .statusCode (400)
            .body (equalTo ("Pick must be latest, between or through."))
          .when
            .get ("/123")
        }}}}}
