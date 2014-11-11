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
import org.scalatest.FreeSpec

import movies.{DisplayModel => DM}

class AnalyticsResourceSpec extends FreeSpec with SpecTools {

  def addMovieAndActor (movies: MovieStore) = {

    val t1 = movies.update (Random.nextXid, t0, "1", """{
      "id": "1",
      "title": "Star Wars",
      "released": null,
      "cast": []
    }""" .fromJson [DM.Movie]) .await()

    val t2 = movies.update (Random.nextXid, t1, "1", """{
    "id": "1",
      "name": "Mark Hamill",
      "born": null,
      "roles": [
        {"movieId": "1", "role": "Luke Skywalker"}
      ]
    }""" .fromJson [DM.Actor]) .await()
  }

  "When the database is empty" - {

    "GET /rdd/movies should respond Ok with no items" in
      served { (port, store) => implicit movies =>
        given
          .port (port)
        .expect
          .body (matchesJson ("[]"))
        .when
          .get ("/rdd/movies")
      }

    "GET /rdd/actors should respond Ok with no items" in
      served { (port, store) => implicit movies =>
        given
          .port (port)
        .expect
          .body (matchesJson ("[]"))
        .when
          .get ("/rdd/actors")
      }

    "GET /rdd/roles should respond Ok with no items" in
      served { (port, store) => implicit movies =>
        given
          .port (port)
        .expect
          .body (matchesJson ("[]"))
        .when
          .get ("/rdd/roles")
      }}

  "When the database has a movie and actor" - {

    "GET /rdd/movies should respond Ok with items" in
      served { (port, store) => implicit movies =>
        addMovieAndActor (movies)
        given
          .port (port)
        .expect
          .body (matchesJson ("""[ {
            "id" : "1",
            "title" : "Star Wars",
            "released" : null
          }]"""))
        .when
          .get ("/rdd/movies")
      }

    "GET /rdd/actors should respond Ok with items" in
      served { (port, store) => implicit movies =>
        addMovieAndActor (movies)
        given
          .port (port)
        .expect
          .body (matchesJson ("""[{
            "id" : "1",
            "name" : "Mark Hamill",
            "born" : null
          }]"""))
        .when
          .get ("/rdd/actors")
      }

    "GET /rdd/roles should respond Ok with items" in
      served { (port, store) => implicit movies =>
        addMovieAndActor (movies)
        given
          .port (port)
        .expect
          .body (matchesJson ("""[{
            "movieId" : "1",
            "actorId" : "1",
            "role" : "Luke Skywalker"
          }]"""))
        .when
          .get ("/rdd/roles")
      }}}
