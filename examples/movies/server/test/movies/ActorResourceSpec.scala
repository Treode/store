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

import scala.util.Random

import com.jayway.restassured.RestAssured.given
import com.jayway.restassured.response.Response
import org.scalatest.FreeSpec

import movies.{PhysicalModel => PM}

class ActorResourceSpec extends FreeSpec with SpecTools {

  val markHamill = """{
    "id": "1",
    "name": "Mark Hamill",
    "born": null,
    "roles": []
  }"""

  val markHammer = """{
    "id": "1",
    "name": "Mark Hammer",
    "born": null,
    "roles": []
  }"""

  def addMarkHamill (port: Int): Response = {
    given
      .port (port)
      .body (markHamill)
    .expect
      .statusCode (200)
    .when
      .put ("/actor/1")
  }

  "When the database is empty" - {

    "GET /actor/1 should respond Not Found" in
      served { (port, store) => implicit movies =>
        given
          .port (port)
        .expect
          .statusCode (404)
        .when
          .get ("/actor/1")
      }

    "PUT /actor/1 should respond Ok with a valueTxClock" in
      served { (port, store) => implicit movies =>
        val r1 = addMarkHamill (port)
        val t1 = r1.valueTxClock
        store.expectCells (PM.MovieTable) ()
        store.expectCells (PM.CastTable) ()
        store.expectCells (PM.ActorTable) (
            ("1", t1, PO.markHamill))
        store.expectCells (PM.RolesTable) (
            ("1", t1, PM.Roles.empty))
        store.expectCells (PM.Index) (
            ("mark hamill", t1, PO.actors ("1")))
      }

    "POST /actor should respond Ok with a valueTxClock" in
      served { (port, store) => implicit movies =>

        val r1 = given
          .port (port)
          .body (markHamill)
        .expect
          .statusCode (201)
        .when
          .post ("/actor")

        val uri = r1.getHeader ("Location")
        val id = uri.substring ("/actor/".length)
        given
          .port (port)
        .expect
          .valueTxClock (r1.valueTxClock)
          .body (matchesJson (s"""{
              "id": "$id",
              "name": "Mark Hamill",
              "born": null,
              "roles": []
            }"""))
          .when
            .get (uri)
      }

    "PUT /actor/1 without a title should respond Bad Requst" in
      served { (port, store) => implicit movies =>

        val r1 = given
          .port (port)
          .body ("{}")
        .expect
          .statusCode (400)
        .when
          .post ("/actor")

        store.expectCells (PM.MovieTable) ()
        store.expectCells (PM.CastTable) ()
        store.expectCells (PM.ActorTable) ()
        store.expectCells (PM.RolesTable) ()
        store.expectCells (PM.Index) ()
      }

    "GET /actor?q=mark should respond Okay" in
      served { (port, store) => implicit movies =>
        val r1 = given
          .port (port)
          .param ("q", "mark")
        .expect
          .body (matchesJson (s"""{"movies": [], "actors": []}"""))
        .when
          .get ("/actor")
      }}

  "When the database has an actor" - {

    "GET /actor/1 should respond Ok" in
      served { (port, store) => implicit movies =>
        val r1 = addMarkHamill (port)
        given
          .port (port)
        .expect
          .valueTxClock (r1.valueTxClock)
          .body (matchesJson (markHamill))
        .when
          .get ("/actor/1")
      }

    "GET /actor/1 with Condition-TxClock:0 should respond Ok" in
      served { (port, store) => implicit movies =>
        val r1 = addMarkHamill (port)
        given
          .port (port)
          .header ("Condition-TxClock", "0")
        .expect
          .valueTxClock (r1.valueTxClock)
          .body (matchesJson (markHamill))
        .when
          .get ("/actor/1")
      }

    "GET /actor/1 with Condition-TxClock:(r1.valueTxClock) should respond Not Modified" in
      served { (port, store) => implicit movies =>
        val r1 = addMarkHamill (port)
        given
          .port (port)
          .header ("Condition-TxClock", r1.valueTxClock.toString)
        .expect
          .statusCode (304)
        .when
          .get ("/actor/1")
      }

    "GET /actor/1 with Request-TxClock:(r1.valueTxClock-1) should respond Not Found" in
      served { (port, store) => implicit movies =>
        val r1 = addMarkHamill (port)
        given
          .port (port)
          .header ("Request-TxClock", (r1.valueTxClock-1).toString)
        .expect
          .statusCode (404)
        .when
          .get ("/actor/1")
      }

    "GET /actor/1 with Request-TxClock:(r1.valueTxClock) should respond Ok" in
      served { (port, store) => implicit movies =>
        val r1 = addMarkHamill (port)
        given
          .port (port)
          .header ("Request-TxClock", (r1.valueTxClock).toString)
        .expect
          .valueTxClock (r1.valueTxClock)
          .body (matchesJson (markHamill))
        .when
          .get ("/actor/1")
      }

    "PUT /actor/1 with should respond Ok with a valueTxClock" in
      served { (port, store) => implicit movies =>

        val r1 = addMarkHamill (port)
        val r2 = given
          .port (port)
          .body (markHammer)
        .when
          .put ("/actor/1")

        val (t1, t2) = (r1.valueTxClock, r2.valueTxClock)
        store.expectCells (PM.MovieTable) ()
        store.expectCells (PM.CastTable) ()
        store.expectCells (PM.ActorTable) (
            ("1", t2, PO.markHammer),
            ("1", t1, PO.markHamill))
        store.expectCells (PM.RolesTable) (
            ("1", t1, PM.Roles.empty))
        store.expectCells (PM.Index) (
            ("mark hamill", t2, None),
            ("mark hamill", t1, PO.actors ("1")),
            ("mark hammer", t2, PO.actors ("1")))
      }

    "PUT /actor/1 with a Condition-TxClock:1 should respond Ok with a valueTxClock" in
      served { (port, store) => implicit movies =>

        val r1 = addMarkHamill (port)
        val r2 = given
          .port (port)
          .header ("Condition-TxClock", r1.valueTxClock.toString)
          .body (markHammer)
        .when
          .put ("/actor/1")

        val (t1, t2) = (r1.valueTxClock, r2.valueTxClock)
        store.expectCells (PM.MovieTable) ()
        store.expectCells (PM.CastTable) ()
        store.expectCells (PM.ActorTable) (
            ("1", t2, PO.markHammer),
            ("1", t1, PO.markHamill))
        store.expectCells (PM.RolesTable) (
            ("1", t1, PM.Roles.empty))
        store.expectCells (PM.Index) (
            ("mark hamill", t2, None),
            ("mark hamill", t1, PO.actors ("1")),
            ("mark hammer", t2, PO.actors ("1")))
      }

    "PUT /actor/1 with a Condition-TxClock:0 should respond Precondition Failed" in
      served { (port, store) => implicit movies =>

        val r1 = addMarkHamill (port)
        val r2 = given
          .port (port)
          .header ("Condition-TxClock", "0")
          .body (markHammer)
        .expect
          .statusCode (412)
        .when
          .put ("/actor/1")

        val t1 = r1.valueTxClock
        store.expectCells (PM.MovieTable) ()
        store.expectCells (PM.CastTable) ()
        store.expectCells (PM.ActorTable) (
            ("1", t1, PO.markHamill))
        store.expectCells (PM.RolesTable) (
            ("1", t1, PM.Roles.empty))
        store.expectCells (PM.Index) (
            ("mark hamill", t1, PO.actors ("1")))
      }

    "GET /actor?q=mark should respond Okay" in
      served { (port, store) => implicit movies =>
        val r1 = addMarkHamill (port)
        given
          .port (port)
          .param ("q", "mark")
        .expect
          .body (matchesJson (s"""{
            "movies": [],
            "actors": [{
              "id": "1",
              "name": "Mark Hamill",
              "born": null
            }]
          }"""))
        .when
          .get ("/actor")
      }}}
