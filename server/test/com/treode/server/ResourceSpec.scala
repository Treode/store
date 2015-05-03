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
import SchemaParser._
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
    val compilerResult = parse ("table table1 { id : 0x1; }; table table2 { id : 0x2; }; table table3 { id : 0x3; }; table table4 { id : 0x4; };") 
    val newSchema = compilerResult match {
      case CompilerSuccess (schema) => schema
      case CompilerFailure (errors) => Schema (new HashMap [String, Long])
    }
    val schematicStore = new SchematicStubStore (store, newSchema)
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

  class JsonMatcher (expected: String) extends TypeSafeMatcher [String] {

    def matchesSafely (actual: String): Boolean =
      expected.fromJson [JsonNode] == actual.fromJson [JsonNode]

    def describeTo (desc: Description): Unit =
      desc.appendText (expected);
  }

  def matchesJson (expected: String): Matcher [String] =
    new JsonMatcher (expected)

  def update (store: SchematicStubStore, ct: TxClock, key: String, value: String, table: String = "table1"): TxClock =
    store.update (
      table,
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

    "GET /table/table1?key=abc with Read-TxClock:0 should respond Not Found" in
      served { case (port, store) =>
        val ts = addData (store)
        given
          .port (port)
          .header ("Read-TxClock", "0")
        .expect
          .statusCode (404)
        .when
          .get ("/table/table1?key=abc")
      }

    "GET /table/table1?key=abc with Read-TxClock:1 should respond Ok" in
      served { case (port, store) =>
        val ts = addData (store)
        given
          .port (port)
          .header ("Read-TxClock", "1")
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
          .header ("Value-TxClock", ts1.toString)
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
          .header ("Value-TxClock", ts1.toString)
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

    "GET /table/table1?until=4 should work" in {
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

      "GET /table/table1 with Read-TxClock:abc should yield Bad Request" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .header ("Read-TxClock", "abc")
          .expect
            .statusCode (400)
            .body (equalTo ("Bad time for Read-TxClock: abc"))
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

    "and gives a bad slice" - {

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
            .get ("/table/123")
        }}}}

  "When post request contains batch-writes" - {

    def addData (table: String, store: SchematicStubStore, key: String, value: String): TxClock =
      update (store, TxClock.MinValue, key, value, table)

    def updateData (table: String, store: SchematicStubStore, key: String, value: String): TxClock =
      update (store, TxClock.now, key, value, table)

    "and has proper uris" - {

      val v1 = "\"v1\""
      val v2 = "\"v2\""
      val v3 = "\"v3\""

      "POST /batch-write with one CREATE should yield Ok" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .contentType ("application/json")
            .body ("""[{"op": "CREATE", "table": "table1", "key": "abc", "obj": "v1"}]""")
          .expect
            .statusCode (200)
          .when
            .post ("/batch-write")
          val valueTxClock = rsp.valueTxClock
          assertSeq (cell ("abc", valueTxClock, v1)) (store.scan ("table1"))
        }}

      "POST /batch-write with one UPDATE should yield Ok" in {
        served { case (port, store) =>
          val ts1 = addData ("table1", store, "abc", v1)
          val rsp = given
            .port (port)
            .contentType ("application/json")
            .body ("""[{"op": "UPDATE", "table": "table1", "key": "abc", "obj": "v2"}]""")
          .expect
            .statusCode (200)
          .when
            .post ("/batch-write")
          val ts2 = rsp.valueTxClock
          assertSeq (cell ("abc", ts2, v2), cell ("abc", ts1, v1)) (store.scan ("table1"))
        }}

      "POST /batch-write with one HOLD should yield Ok" in {
        served { case (port, store) =>
          val ts1 = addData ("table1", store, "abc", v1)
          val ts2 = updateData ("table1", store, "abc", v2)
          val rsp = given
            .port (port)
            .contentType ("application/json")
            .header ("Condition-TxClock", ts2 .toString)
            .body ("""[{"op": "HOLD", "table": "table1", "key": "abc"}]""")
          .expect
            .statusCode (200)
          .when
            .post ("/batch-write")
        }}

      "POST /batch-write with one delete should yield Ok" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .contentType ("application/json")
            .body ("""[{"op": "DELETE", "table": "table1", "key": "abc"}]""")
          .expect
            .statusCode (200)
          .when
            .post ("/batch-write")
          val valueTxClock = rsp.valueTxClock
          assertSeq (cell ("abc", valueTxClock)) (store.scan ("table1"))
        }}

      "POST /batch-write with mixed case operations should yield Ok" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .contentType ("application/json")
            .body ("""[{"op": "CrEaTe", "table": "table1", "key": "abc", "obj": "v1"}]""")
          .expect
            .statusCode (200)
          .when
            .post ("/batch-write")
          val valueTxClock = rsp.valueTxClock
          assertSeq (cell ("abc", valueTxClock, v1)) (store.scan ("table1"))
        }}

      "POST /batch-write with multiple operations should yeild Ok" in {
        served { case (port, store) =>
          val ts1 = addData ("table2", store, "abc2", v1)
          val ts3 = addData ("table3", store, "abc3", v1)
          val ts4 = updateData ("table3", store, "abc3", v1)
          val rsp = given
            .port (port)
            .contentType ("application/json")
            .header ("Condition-TxClock", ts4 .toString)
            .body ("""
                [ {"op": "CREATE", "table": "table1", "key": "abc", "obj": "v1"},
                  {"op": "UPDATE", "table": "table2", "key": "abc2", "obj": "v3"},
                  {"op": "HOLD", "table": "table3", "key": "abc3"},
                  {"op": "DELETE", "table": "table4", "key": "abc4"} ]""")
          .expect
            .statusCode (200)
          .when
            .post ("/batch-write")
          val ts2 = rsp.valueTxClock
          assertSeq (cell ("abc", ts2, v1)) (store.scan ("table1"))
          assertSeq (cell ("abc2", ts2, v3), cell ("abc2", ts1, v1)) (store.scan ("table2"))
          assertSeq (cell ("abc4", ts2)) (store.scan ("table4"))
      }}}

    "and has improper/failing uris" - {

      "POST /batch-write with bad json format body should yield Bad Request" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .contentType ("application/json")
            .body ("""["op": "CREATE", "tble": "table1", "key": "abc", "obj": "v1"}]""")
          .expect
            .statusCode (400)
          .when
            .post ("/batch-write")
        }}

      "POST /batch-write with necessary fields missing should yield Bad Request" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .contentType ("application/json")
            .body ("""[{"op": "CREATE", "tble": "table1", "key":"abc", "obj": "v1"}]""")
          .expect
            .statusCode (400)
            .body (equalTo ("There is no attribute called 'table'"))
          .when
            .post ("/batch-write")
        }}

      "POST /batch-write with undefined operation should yield Bad Request" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .contentType ("application/json")
            .body ("""[{"op": "SCAN", "table": "table1", "key": "abc", "obj": "v1"}]""")
          .expect
            .statusCode (400)
            .body (equalTo ("Unsupported operation: SCAN"))
          .when
            .post ("/batch-write")
        }}

      "PUT /batch-write with wrong request method (PUT) should yield Method Not Allowed" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .contentType ("application/json")
            .body ("""[{"op": "CREATE", "table": "table1", "key": "abc", "obj": "v1"}]""")
          .expect
            .statusCode (405)
          .when
            .put ("/batch-write")
        }}

      "POST /batch-write with unspecified table name should yield Bad Request" in {
        served { case (port, store) =>
          val rsp = given
            .port (port)
            .contentType ("application/json")
            .body ("""[{"op": "CREATE", "table": "table5", "key": "abc", "obj": "v1"}]""")
          .expect
            .statusCode (400)
            .body (equalTo ("Bad table ID: table5"))
          .when
            .post ("/batch-write")
        }}

      "POST /batch-write requesting HOLD after another client's write should yield Precondition Failed" in {
        served { case (port, store) =>
          val ts1 = addData ("table1", store, "abc", """{"v":"ant"}""")
          val ts2 = updateData ("table1", store, "abc", """{"v":"ant2"}""")
          val rsp = given
            .port (port)
            .contentType ("application/json")
            .header ("Condition-TxClock", ts1 .toString)
            .body ("""[{"op": "HOLD", "table": "table1", "key": "abc"}]""")
          .expect
            .statusCode (412)
            .header ("Value-TxClock", ts2.toString)
          .when
            .post ("/batch-write")
        }}

      "POST /batch-write has 2 operations on the same table and key should yield Bad Request" in {
        served { case (port, store) =>
          val body = """{"v":"ant2"}"""
          val rsp = given
            .port (port)
            .contentType ("application/json")
            .body (
              """[{"op": "CREATE", "table": "table1", "key": "abc", "obj": "v1"},""" +
              """{"op": "UPDATE", "table": "table1", "key": "abc", "obj": "v2"}]""")
          .expect
            .statusCode (400)
            .body (equalTo ("Multiple (Table, key) pairs found"))
          .when
            .post ("/batch-write")
    }}}}}
