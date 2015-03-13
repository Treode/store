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

class MovieResourceSpec extends FreeSpec with SpecTools {

  val starWars = """{
    "id": "1",
    "title": "Star Wars",
    "released": null,
    "cast": []
  }"""

  val aNewHope = """{
    "id": "1",
    "title": "Star Wars: A New Hope",
    "released": null,
    "cast": []
  }"""

  def addStarWars (port: Int) = {
    given
      .port (port)
      .body (starWars)
    .expect
      .statusCode (200)
    .when
      .put ("/movie/1")
  }

  "When the database is empty" - {

    "GET /movie/1 should respond Not Found" in
      served { (port, store) => implicit movies =>
        given
          .port (port)
        .expect
          .statusCode (404)
        .when
          .get ("/movie/1")
      }

    "PUT /movie/1 should respond Ok with a valueTxClock" in
      served { (port, store) => implicit movies =>
        val r1 = addStarWars (port)
        val t1 = r1.valueTxClock
        store.expectCells (PM.MovieTable) (
            ("1", t1, PO.starWars))
        store.expectCells (PM.CastTable) (
            ("1", t1, PM.Cast.empty))
        store.expectCells (PM.ActorTable) ()
        store.expectCells (PM.RolesTable) ()
        store.expectCells (PM.Index) (
            ("star wars", t1, PO.movies ("1")))
      }

    "POST /movie should respond Ok with a valueTxClock" in
      served { (port, store) => implicit movies =>

        val r1 = given
          .port (port)
          .body (starWars)
        .expect
          .statusCode (201)
        .when
          .post ("/movie")

        val uri = r1.getHeader ("Location")
        val id = uri.substring ("/movie/".length)
        given
          .port (port)
        .expect
          .valueTxClock (r1.valueTxClock)
          .body (matchesJson (s"""{
              "id": "$id",
              "title": "Star Wars",
              "released" :null,
              "cast": []
            }"""))
          .when
            .get (uri)
      }

    "PUT /movie/1 without a title should respond Bad Requst" in
      served { (port, store) => implicit movies =>

        val r1 = given
          .port (port)
          .body ("{}")
        .expect
          .statusCode (400)
        .when
          .post ("/movie")

        store.expectCells (PM.MovieTable) ()
        store.expectCells (PM.CastTable) ()
        store.expectCells (PM.ActorTable) ()
        store.expectCells (PM.RolesTable) ()
        store.expectCells (PM.Index) ()
      }

    "GET /movie?q=star should respond Okay" in
      served { (port, store) => implicit movies =>
        val r1 = given
          .port (port)
          .param ("q", "star")
        .expect
          .body (matchesJson (s"""{"movies": [], "actors": []}"""))
        .when
          .get ("/movie")
      }}

  "When the database has a movie" - {

    "GET /movie/1 should respond Ok" in
      served { (port, store) => implicit movies =>
        val r1 = addStarWars (port)
        given
          .port (port)
        .expect
          .valueTxClock (r1.valueTxClock)
          .body (matchesJson (starWars))
        .when
          .get ("/movie/1")
      }

    "GET /movie/1 with Condition-TxClock:0 should respond Ok" in
      served { (port, store) => implicit movies =>
        val r1 = addStarWars (port)
        given
          .port (port)
          .header ("Condition-TxClock", "0")
        .expect
          .valueTxClock (r1.valueTxClock)
          .body (matchesJson (starWars))
        .when
          .get ("/movie/1")
      }

    "GET /movie/1 with Condition-TxClock:(r1.valueTxClock) should respond Not Modified" in
      served { (port, store) => implicit movies =>
        val r1 = addStarWars (port)
        given
          .port (port)
          .header ("Condition-TxClock", r1.valueTxClock.time.toString)
        .expect
          .statusCode (304)
        .when
          .get ("/movie/1")
      }

    "GET /movie/1 with Request-TxClock:(r1.valueTxClock-1) should respond Not Found" in
      served { (port, store) => implicit movies =>
        val r1 = addStarWars (port)
        given
          .port (port)
          .header ("Request-TxClock", (r1.valueTxClock-1).time.toString)
        .expect
          .statusCode (404)
        .when
          .get ("/movie/1")
      }

    "GET /movie/1 with Request-TxClock:(r1.valueTxClock) should respond Ok" in
      served { (port, store) => implicit movies =>
        val r1 = addStarWars (port)
        given
          .port (port)
          .header ("Request-TxClock", (r1.valueTxClock).time.toString)
        .expect
          .valueTxClock (r1.valueTxClock)
          .body (matchesJson (starWars))
        .when
          .get ("/movie/1")
      }

    "PUT /movie/1 with should respond Ok with a valueTxClock" in
      served { (port, store) => implicit movies =>

        val r1 = addStarWars (port)
        val r2 = given
          .port (port)
          .body (aNewHope)
        .when
          .put ("/movie/1")

        val (t1, t2) = (r1.valueTxClock, r2.valueTxClock)
        store.expectCells (PM.MovieTable) (
            ("1", t2, PO.aNewHope),
            ("1", t1, PO.starWars))
        store.expectCells (PM.CastTable) (
            ("1", t1, PM.Cast.empty))
        store.expectCells (PM.ActorTable) ()
        store.expectCells (PM.RolesTable) ()
        store.expectCells (PM.Index) (
            ("star wars", t2, None),
            ("star wars", t1, PO.movies ("1")),
            ("star wars: a new hope", t2, PO.movies ("1")))
      }

    "PUT /movie/1 with a Condition-TxClock:1 should respond Ok with a valueTxClock" in
      served { (port, store) => implicit movies =>

        val r1 = addStarWars (port)
        val r2 = given
          .port (port)
          .header ("Condition-TxClock", r1.valueTxClock.toString)
          .body (aNewHope)
        .when
          .put ("/movie/1")

        val (t1, t2) = (r1.valueTxClock, r2.valueTxClock)
        store.expectCells (PM.MovieTable) (
            ("1", t2, PO.aNewHope),
            ("1", t1, PO.starWars))
        store.expectCells (PM.CastTable) (
            ("1", t1, PM.Cast.empty))
        store.expectCells (PM.ActorTable) ()
        store.expectCells (PM.RolesTable) ()
        store.expectCells (PM.Index) (
            ("star wars", t2, None),
            ("star wars", t1, PO.movies ("1")),
            ("star wars: a new hope", t2, PO.movies ("1")))
    }

    "PUT /movie/1 with a Condition-TxClock:0 should respond Precondition Failed" in
      served { (port, store) => implicit movies =>

        val r1 = addStarWars (port)
        val r2 = given
          .port (port)
          .header ("Condition-TxClock", "0")
          .body (aNewHope)
        .expect
          .statusCode (412)
        .when
          .put ("/movie/1")

        val t1 = r1.valueTxClock
        store.expectCells (PM.MovieTable) (
            ("1", t1, PO.starWars))
        store.expectCells (PM.CastTable) (
            ("1", t1, PM.Cast.empty))
        store.expectCells (PM.ActorTable) ()
        store.expectCells (PM.RolesTable) ()
        store.expectCells (PM.Index) (
            ("star wars", t1, PO.movies ("1")))
      }

    "GET /movie?q=star should respond Okay" in
      served { (port, store) => implicit movies =>
        val r1 = addStarWars (port)
        given
          .port (port)
          .param ("q", "star")
        .expect
          .body (matchesJson (s"""{
            "movies": [{
              "id": "1",
              "title": "Star Wars",
              "released": null
            }],
            "actors": []
          }"""))
        .when
          .get ("/movie")
      }}}
