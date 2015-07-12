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
import com.treode.store.TxClock
import org.hamcrest.Matchers._
import org.scalatest.FreeSpec

import movies.{DisplayModel => DM}

class SearchResourceSpec extends FreeSpec with SpecTools {

  def addTitle (ct: TxClock, title: String) (implicit movies: MovieStore): (String, TxClock) =
    movies
      .create (Random.nextXid, ct, ct, DM.Movie ("", title, null, null))
      .await()

  def addName (ct: TxClock, name: String) (implicit movies: MovieStore): (String, TxClock) =
    movies
      .create (Random.nextXid, ct, ct, DM.Actor ("", name, null, null))
      .await()

  "When the database is empty" - {

    "GET /search?q=johnny should respond Okay" in
      served { (port, store) => implicit movies =>
        given
          .port (port)
          .param ("q", "johnny")
        .expect
          .body (matchesJson (s"""{"movies": [], "actors": []}"""))
        .when
          .get ("/search")
      }}

  "When the database has a movie and and actor" - {

    "GET /search?q=johnny should respond Okay" in
      served { (port, store) => implicit movies =>

        val (id1, t1) = addTitle (t0, "Johnny Bravo")
        val (id2, t2) = addTitle (t0, "Star Wars")
        val (id3, t3) = addName (t0, "Johnny Depp")
        val (id4, t4) = addName (t0, "Mark Hamill")
        val t = Seq (t1, t2, t3, t4) .max

        given
          .port (port)
          .param ("q", "johnny")
        .expect
          .body (matchesJson (s"""{
            "movies": [{
              "id": "$id1",
              "title": "Johnny Bravo",
              "released": null
            }],
            "actors": [{
              "id": "$id3",
              "name": "Johnny Depp",
              "born": null
            }]
          }"""))
        .when
          .get ("/search")
      }}

  "When the user is cantankerous" - {

    "GET /search should respond BadRequest" in
      served { (port, store) => implicit movies =>
        given
          .port (port)
        .expect
          .statusCode (400)
          .body (equalTo ("Query parameter q is required."))
        .when
          .get ("/search")
      }}}
