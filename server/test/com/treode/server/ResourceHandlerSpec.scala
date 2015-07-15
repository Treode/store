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
import scala.collection.mutable.HashMap

import com.fasterxml.jackson.databind.JsonNode
import com.jayway.restassured.RestAssured.given
import com.jayway.restassured.response.{Response => RestAssuredResponse}
import com.jayway.restassured.specification.{RequestSpecification, ResponseSpecification}
import com.treode.async.stubs.{StubGlobals, StubScheduler}, StubGlobals.scheduler
import com.treode.store.{Bytes, Cell, TxClock, WriteOp, StaleException, TxId, Key}
import com.treode.store.stubs.StubStore
import com.treode.twitter.finagle.http.filter._
import com.twitter.finagle.Http
import org.hamcrest.{Description, Matcher, Matchers, TypeSafeMatcher}, Matchers._
import org.scalatest.FreeSpec

import ResourceHandler._

class ResourceHandlerSpec extends FreeSpec {

  val v1 = "\"v1\""
  val v2 = "\"v2\""
  val v3 = "\"v3\""

  val SchemaParser.CompilerSuccess (schema) = SchemaParser.parse ("""
    table table1 { id : 0x1; };
    table table2 { id : 0x2; };
    table table3 { id : 0x3; };
    table table4 { id : 0x4; };
  """)

  def served (test: (Int, StubSchematicStore) => Any) {
    val store = StubStore()
    val stub = new StubSchematicStore (store, schema)
    val librarian = new StubLibrarian (schema)
    val port = Random.nextInt (65535 - 49152) + 49152
    val server = Http.serve (
      s":$port",
      NettyToFinagle andThen
      BadRequestFilter andThen
      JsonExceptionFilter andThen
      new ResourceHandler (store, librarian))
    try {
      test (port, stub)
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

  implicit class RichRequestSpec (req: RequestSpecification) {

    def conditionTxClock (time: TxClock): RequestSpecification =
      req.header ("Condition-TxClock", time.time.toString)

    def readTxClock (time: TxClock): RequestSpecification =
      req.header ("Read-TxClock", time.time.toString)

    def tx (id: Int): RequestSpecification =
      req.header ("Transaction-Id", id.toString)
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

  "Resource.route" - {
    "should route properly" in {
      assertResult (Table ("tab")) (route ("/tab"))
      assertResult (Table ("tab")) (route ("/tab/"))
      assertResult (Row ("tab", "key")) (route ("/tab/key"))
      assertResult (Batch) (route ("/batch-write"))
      assertResult (Status) (route ("/tx-status"))
      assertResult (Unmatched) (route ("/"))
      assertResult (Unmatched) (route ("/tab/key/"))
    }}

  "When the database is empty" - {

    "GET /table1/abc should respond Not Found" in
      served { case (port, store) =>
        given
          .port (port)
        .expect
          .statusCode (404)
        .when
          .get ("/table1/abc")
      }

    "POST /table1/abc should respond Ok and keep the status" in
      served { case (port, store) =>

        val rsp = given
          .port (port)
          .tx (1)
          .body (v1)
        .expect
          .statusCode (200)
        .when
          .post ("/table1/abc")
        val time = rsp.valueTxClock
        assertSeq (cell ("abc", time, v1)) (store.scan ("table1"))

        val status = given
          .port (port)
          .tx (1)
        .expect
          .statusCode (200)
        .when
          .get ("/tx-status")
        assertResult (time) (rsp.valueTxClock)
      }

    "PUT /table1/abc should respond Ok" in
      served { case (port, store) =>
        val rsp = given
          .port (port)
          .body (v1)
        .expect
          .statusCode (200)
        .when
          .put ("/table1/abc")
        val time = rsp.valueTxClock
        assertSeq (cell ("abc", time, v1)) (store.scan ("table1"))
      }

    "DELETE /table1/abc should respond Ok" in
      served { case (port, store) =>
        val body = v1
        val rsp = given
          .port (port)
          .body (body)
        .expect
          .statusCode (200)
        .when
          .delete ("/table1/abc")
        val time = rsp.valueTxClock
        assertSeq (cell ("abc", time)) (store.scan ("table1"))
      }}

  "When the database has an entry" - {

    def addData (store: StubSchematicStore): TxClock =
      store.update (TxClock.MinValue, "table1", "abc", v1) .await

    "GET /table1/abc should respond Ok" in
      served { case (port, store) =>
        val ts = addData (store)
        given
          .port (port)
        .expect
          .statusCode (200)
          .body (equalTo (v1))
        .when
          .get ("/table1/abc")
      }

    "GET /table1/abc with Condition-TxClock:0 should respond Ok" in
      served { case (port, store) =>
        val ts = addData (store)
        given
          .port (port)
          .conditionTxClock (ts - 1)
        .expect
          .statusCode (200)
          .body (equalTo (v1))
        .when
          .get ("/table1/abc")
      }

    "GET /table1/abc with Condition-TxClock:1 should respond Not Modified" in
      served { case (port, store) =>
        val ts = addData (store)
        given
          .port (port)
          .conditionTxClock (ts)
        .expect
          .statusCode (304)
        .when
          .get ("/table1/abc")
      }

    "GET /table1/abc with Read-TxClock:0 should respond Not Found" in
      served { case (port, store) =>
        val ts = addData (store)
        given
          .port (port)
          .readTxClock (ts - 1)
        .expect
          .statusCode (404)
        .when
          .get ("/table1/abc")
      }

    "GET /table1/abc with Read-TxClock:1 should respond Ok" in
      served { case (port, store) =>
        val ts = addData (store)
        given
          .port (port)
          .readTxClock (ts)
        .expect
          .statusCode (200)
          .body (equalTo (v1))
        .when
          .get ("/table1/abc")
      }

    "POST /table1/abc should respond Conflict and keep the status" in
      served { case (port, store) =>

        val ts1 = addData (store)

        val rsp = given
          .port (port)
          .tx (1)
          .body (v2)
        .expect
          .statusCode (409)
        .when
          .post ("/table1/abc")
        assertSeq (cell ("abc", ts1, v1)) (store.scan ("table1"))

        given
          .port (port)
          .tx (1)
        .expect
          .statusCode (400)
        .when
          .get ("/tx-status")
      }

    "POST /table1/abc with If-Unmodified-Since:1 should respond Conflict" in
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .conditionTxClock (ts1)
          .body (v2)
        .expect
          .statusCode (409)
        .when
          .post ("/table1/abc")
        assertSeq (cell ("abc", ts1, v1)) (store.scan ("table1"))
      }

    "POST /table1/abc with If-Unmodified-Since:0 should respond Conflict" in
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .conditionTxClock (ts1 - 1)
          .body (v2)
        .expect
          .statusCode (409)
        .when
          .post ("/table1/abc")
        assertSeq (cell ("abc", ts1, v1)) (store.scan ("table1"))
      }

    "PUT /table1/abc should respond Ok" in
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .body (v2)
        .expect
          .statusCode (200)
        .when
          .put ("/table1/abc")
        val ts2 = rsp.valueTxClock
        assertSeq (cell ("abc", ts2, v2), cell ("abc", ts1, v1)) (store.scan ("table1"))
      }

    "PUT /table1/abc with If-Unmodified-Since:1 should respond Ok" in
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .conditionTxClock (ts1)
          .body (v2)
        .expect
          .statusCode (200)
        .when
          .put ("/table1/abc")
        val ts2 = rsp.valueTxClock
        assertSeq (cell ("abc", ts2, v2), cell ("abc", ts1, v1)) (store.scan ("table1"))
      }

    "PUT /table1/abc with If-Unmodified-Since:0 should respond Precondition Failed" in
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .conditionTxClock (ts1 - 1)
          .body (v2)
        .expect
          .statusCode (412)
          .valueTxClock (ts1)
        .when
          .put ("/table1/abc")
        assertSeq (cell ("abc", ts1, v1)) (store.scan ("table1"))
      }

    "DELETE /table1/abc should respond Ok" in
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
        .expect
          .statusCode (200)
        .when
          .delete ("/table1/abc")
        val ts2 = rsp.valueTxClock
        assertSeq (cell ("abc", ts2), cell ("abc", ts1, v1)) (store.scan ("table1"))
      }

    "DELETE /table1/abc with If-Unmodified-Since:1 should respond Ok" in
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .conditionTxClock (ts1)
        .expect
          .statusCode (200)
        .when
          .delete ("/table1/abc")
        val ts2 = rsp.valueTxClock
        assertSeq (cell ("abc", ts2), cell ("abc", ts1, v1)) (store.scan ("table1"))
      }

    "DELETE /table1/abc with If-Unmodified-Since:0 should respond Precondition Failed" in
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .conditionTxClock (ts1 - 1)
        .expect
          .statusCode (412)
          .valueTxClock (ts1)
        .when
          .delete ("/table1/abc")
        assertSeq (cell ("abc", ts1, v1)) (store.scan ("table1"))
      }}

  "When the database has a deleted entry" - {
    "POST /table1/abc with If-Unmodified-Since:2 should respond Ok" in
      served { case (port, store) =>
        val ts1 = store.update (TxClock.MinValue, "table1", "abc", v1) .await
        val ts2 = store.delete (ts1, "table1", "abc") .await
        val rsp = given
          .port (port)
          .conditionTxClock (ts2)
          .body (v2)
        .expect
          .statusCode (200)
        .when
          .post ("/table1/abc")
        val ts3 = rsp.valueTxClock
        assertSeq (
          cell ("abc", ts3, v2),
          cell ("abc", ts2),
          cell ("abc", ts1, v1)
        ) (store.scan ("table1"))
      }

    "POST /table1/abc with If-Unmodified-Since:0 should respond Ok" in
      served { case (port, store) =>
        val ts1 = store.update (TxClock.MinValue, "table1", "abc", v1) .await
        val ts2 = store.delete (ts1, "table1", "abc") .await
        val rsp = given
          .port (port)
          .conditionTxClock (TxClock.MinValue)
          .body (v2)
        .expect
          .statusCode (200)
        .when
          .post ("/table1/abc")
        val ts3 = rsp.valueTxClock
        assertSeq (
            cell ("abc", ts3, v2),
            cell ("abc", ts2),
            cell ("abc", ts1, v1)
        ) (store.scan ("table1"))
      }}

  "When the database has history" - {

    def addData (store: StubSchematicStore) {
      var t = TxClock.MinValue
      t = store.update (t, "table1", "a", "\"a1\"") .await
      t = store.update (t, "table1", "a", "\"a2\"") .await
      t = store.update (t, "table1", "b", "\"b1\"") .await
      t = store.delete (t, "table1", "b") .await
      t = store.update (t, "table1", "c", "\"c1\"") .await
    }

    "GET /table1 should work" in
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .expect
          .statusCode (200)
          .body (matchesJson ("""[
            {"key": "a", "time": 2, "value": "a2"},
            {"key": "c", "time": 5, "value": "c1"}
          ]"""))
        .when
          .get ("/table1")
      }

    "GET /table1?until=4 should work" in
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .param ("until", "4")
        .expect
          .statusCode (200)
          .body (matchesJson ("""[
            {"key": "a", "time": 2, "value": "a2"}
          ]"""))
        .when
          .get ("/table1")
      }

    "GET /table1?until=4&pick=between should work" in
      served { case (port, store) =>
        val ts1 = addData (store)
        val rsp = given
          .port (port)
          .param ("until", "4")
          .param ("pick", "between")
        .expect
          .statusCode (200)
          .body (matchesJson ("""[
            {"key": "a", "time": 2, "value": "a2"},
            {"key": "a", "time": 1, "value": "a1"},
            {"key": "b", "time": 4, "value": null},
            {"key": "b", "time": 3, "value": "b1"}
          ]"""))
        .when
          .get ("/table1")
      }

    "GET /table1?slice=0&nslices=2 should work" in
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
          .get ("/table1")
      }}

  "When the user is cantankerous" - {

    "and gives bad URIs" - {

      "GET /non-table should yield Bad Request" in
        served { case (port, store) =>
          val rsp = given
            .port (port)
          .expect
            .statusCode (404)
          .when
            .get ("/non-table")
        }

      "PUT /table1 should yield Method Not Allowed" in
        served { case (port, store) =>
          val rsp = given
            .port (port)
          .expect
            .statusCode (405)
          .when
            .put ("/table1")
        }

      "DELETE /table1 should yield Method Not Allowed" in
        served { case (port, store) =>
          val rsp = given
            .port (port)
          .expect
            .statusCode (405)
          .when
            .delete ("/table1")
        }}

    "and gives bad clock values" - {

      "GET /table1/abc with Read-TxClock:abc should yield Bad Request" in
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .header ("Read-TxClock", "abc")
          .expect
            .statusCode (400)
            .body (equalTo ("Bad time for Read-TxClock: abc"))
          .when
            .get ("/table1/abc")
        }

      "GET /table1/abc with Condition-TxClock:abc should yield Bad Request" in
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .header ("Condition-TxClock", "abc")
          .expect
            .statusCode (400)
            .body (equalTo ("Bad time for Condition-TxClock: abc"))
          .when
            .get ("/table1/abc")
        }

      "PUT /table1/abc with If-Unmodified-Since:abc should yield Bad Request" in
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .header ("Condition-TxClock", "abc")
          .expect
            .statusCode (400)
            .body (equalTo ("Bad time for Condition-TxClock: abc"))
          .when
            .put ("/table1/abc")
        }

      "DELETE /table1/abc with If-Unmodified-Since:abc should yield Bad Request" in
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .header ("Condition-TxClock", "abc")
          .expect
            .statusCode (400)
            .body (equalTo ("Bad time for Condition-TxClock: abc"))
          .when
            .delete ("/table1/abc")
        }}

    "and gives a bad slice" - {

      "GET /table1?slice=abc&nslices=2 should yield Bad Request" in
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .param ("slice", "abc")
            .param ("nslices", "2")
          .expect
            .statusCode (400)
            .body (equalTo ("Bad integer for slice: abc"))
          .when
            .get ("/table1")
        }}

    "and gives a bad window" - {

      "GET /table1?pick=foobar should yield Bad Request" in
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .param ("pick", "foobar")
          .expect
            .statusCode (400)
            .body (equalTo ("Pick must be latest, between or through."))
          .when
            .get ("/table1")
        }}}

  "When post request contains batch-writes" - {

    def addData (store: StubSchematicStore, table: String, key: String, value: String): TxClock =
      store.update (TxClock.MinValue, table, key, value) .await

    def updateData (store: StubSchematicStore, ct: TxClock, table: String, key: String, value: String): TxClock =
      store.update (ct, table, key, value) .await

    "and has proper JSON" - {

      "POST /batch-write with one CREATE should yield Ok" in
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .contentType ("application/json")
            .body ("""[{"op": "CREATE", "table": "table1", "key": "abc", "value": "v1"}]""")
          .expect
            .statusCode (200)
          .when
            .post ("/batch-write")
          val time = rsp.valueTxClock
          assertSeq (cell ("abc", time, v1)) (store.scan ("table1"))
        }

      "POST /batch-write with one UPDATE should yield Ok" in
        served { case (port, store) =>
          val ts1 = addData (store, "table1", "abc", v1)
          val rsp = given
            .port (port)
            .contentType ("application/json")
            .body ("""[{"op": "UPDATE", "table": "table1", "key": "abc", "value": "v2"}]""")
          .expect
            .statusCode (200)
          .when
            .post ("/batch-write")
          val ts2 = rsp.valueTxClock
          assertSeq (cell ("abc", ts2, v2), cell ("abc", ts1, v1)) (store.scan ("table1"))
        }

      "POST /batch-write with one DELETE should yield Ok" in
        served { case (port, store) =>
          val ts1 = addData (store, "table1", "abc", v1)
          val rsp = given
            .port (port)
            .contentType ("application/json")
            .body ("""[{"op": "DELETE", "table": "table1", "key": "abc"}]""")
          .expect
            .statusCode (200)
          .when
            .post ("/batch-write")
          val time = rsp.valueTxClock
          assertSeq (cell ("abc", time), cell ("abc", ts1, v1)) (store.scan ("table1"))
        }

      "POST /batch-write with mixed case operations should yield Ok" in
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .contentType ("application/json")
            .body ("""[{"op": "CrEaTe", "table": "table1", "key": "abc", "value": "v1"}]""")
          .expect
            .statusCode (200)
          .when
            .post ("/batch-write")
          val time = rsp.valueTxClock
          assertSeq (cell ("abc", time, v1)) (store.scan ("table1"))
        }

      "POST /batch-write with multiple operations should yeild Ok" in
        served { case (port, store) =>
          val ts1 = addData (store, "table2", "abc2", v1)
          val ts3 = addData (store, "table3", "abc3", v1)
          val ts4 = updateData (store, ts3, "table3", "abc3", v2)
          val rsp = given
            .port (port)
            .contentType ("application/json")
            .conditionTxClock (ts4)
            .body ("""
                [ {"op": "CREATE", "table": "table1", "key": "abc", "value": "v1"},
                  {"op": "UPDATE", "table": "table2", "key": "abc2", "value": "v3"},
                  {"op": "HOLD", "table": "table3", "key": "abc3"},
                  {"op": "DELETE", "table": "table4", "key": "abc4"} ]""")
          .expect
            .statusCode (200)
          .when
            .post ("/batch-write")
          val ts2 = rsp.valueTxClock
          assertSeq (cell ("abc", ts2, v1)) (store.scan ("table1"))
          assertSeq (cell ("abc2", ts2, v3), cell ("abc2", ts1, v1)) (store.scan ("table2"))
          assertSeq (cell ("abc3", ts4, v2), cell ("abc3", ts3, v1)) (store.scan ("table3"))
          assertSeq (cell ("abc4", ts2)) (store.scan ("table4"))
      }

      "POST /batch-write requesting CREATE of an existing key should yield Conflict" in
        served { case (port, store) =>
          val ts1 = addData (store, "table1", "abc", v1)
          val rsp = given
            .port (port)
            .contentType ("application/json")
            .body ("""[{"op": "CREATE", "table": "table1", "key": "abc", "value": "v2"}]""")
          .expect
            .statusCode (409)
          .when
            .post ("/batch-write")
          assertSeq (cell ("abc", ts1, v1)) (store.scan ("table1"))
        }

      "POST /batch-write requesting HOLD after another client's write should yield Precondition Failed" in
        served { case (port, store) =>
          val ts1 = addData (store, "table1", "abc", v1)
          val ts2 = updateData (store, ts1, "table1", "abc", v2)
          val rsp = given
            .port (port)
            .contentType ("application/json")
            .conditionTxClock (ts1)
            .body ("""
                [ {"op": "HOLD", "table": "table1", "key": "abc"},
                  {"op": "UPDATE", "table": "table1", "key": "abc2", "value": "v1"} ]""")
          .expect
            .statusCode (412)
            .valueTxClock (ts2)
            .body (equalTo (""))
          .when
            .post ("/batch-write")
      }}

    "and has improper JSON" - {

      "POST /batch-write with bad JSON should yield Bad Request" in
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .contentType ("application/json")
            .body ("""[}""")
          .expect
            .statusCode (400)
            .body (containsString ("Unexpected close marker '}'"))
          .when
            .post ("/batch-write")
        }

      "POST /batch-write with an empty array should yield Bad Request" in
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .contentType ("application/json")
            .body ("""[]""")
          .expect
            .statusCode (400)
            .body (equalTo ("Batch must have some writes."))
          .when
            .post ("/batch-write")
        }

      "POST /batch-write with one HOLD should yield Bad Request" in
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .contentType ("application/json")
            .body ("""[{"op": "HOLD", "table": "table1", "key": "abc"}]""")
          .expect
            .statusCode (400)
            .body (equalTo ("Batch must have some writes."))
          .when
            .post ("/batch-write")
        }

      "POST /batch-write with a missing op should yield Bad Request" in
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .contentType ("application/json")
            .body ("""[{"table": "table1", "key": "abc", "value": "v1"}]""")
          .expect
            .statusCode (400)
            .body (equalTo ("There is no attribute called 'op'"))
          .when
            .post ("/batch-write")
        }

      "POST /batch-write with a missing table should yield Bad Request" in
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .contentType ("application/json")
            .body ("""[{"op": "CREATE", "key": "abc", "value": "v1"}]""")
          .expect
            .statusCode (400)
            .body (equalTo ("There is no attribute called 'table'"))
          .when
            .post ("/batch-write")
        }

      "POST /batch-write with a missing key should yield Bad Request" in
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .contentType ("application/json")
            .body ("""[{"op": "CREATE", "table": "table1", "value": "v1"}]""")
          .expect
            .statusCode (400)
            .body (equalTo ("There is no attribute called 'key'"))
          .when
            .post ("/batch-write")
        }

      "POST /batch-write with a missing object should yield Bad Request" in
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .contentType ("application/json")
            .body ("""[{"op": "CREATE", "table": "table1", "key": "abc"}]""")
          .expect
            .statusCode (400)
            .body (equalTo ("There is no attribute called 'value'"))
          .when
            .post ("/batch-write")
        }

      "POST /batch-write with an undefined operation should yield Bad Request" in
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .contentType ("application/json")
            .body ("""[{"op": "BAD", "table": "table1", "key": "abc", "value": "v1"}]""")
          .expect
            .statusCode (400)
            .body (equalTo ("""Unsupported operation: "BAD"."""))
          .when
            .post ("/batch-write")
        }

      "PUT /batch-write with wrong request method (PUT) should yield Method Not Allowed" in
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .contentType ("application/json")
            .body ("""[{"op": "CREATE", "table": "table1", "key": "abc", "value": "v1"}]""")
          .expect
            .statusCode (405)
          .when
            .put ("/batch-write")
        }

      "POST /batch-write with non-existent table name should yield Bad Request" in
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .contentType ("application/json")
            .body ("""[{"op": "CREATE", "table": "table5", "key": "abc", "value": "v1"}]""")
          .expect
            .statusCode (400)
            .body (equalTo ("Bad table ID: table5"))
          .when
            .post ("/batch-write")
        }

      "POST /batch-write has 2 operations on the same table and key should yield Bad Request" in
        served { case (port, store) =>
          val body = """{"v":"ant2"}"""
          val rsp = given
            .port (port)
            .contentType ("application/json")
            .body ("""
                [ {"op": "CREATE", "table": "table1", "key": "abc", "value": "v1"},
                  {"op": "UPDATE", "table": "table1", "key": "abc", "value": "v2"} ]""")
          .expect
            .statusCode (400)
            .body (equalTo ("""Multiple rows found for "table1:abc"."""))
          .when
            .post ("/batch-write")
    }}}}
