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

import com.treode.async.stubs.StubScheduler
import com.treode.store.stubs.StubStore
import com.twitter.finagle.http.MediaType
import com.twitter.finatra.test.MockApp
import org.scalatest.{FreeSpec, Matchers}

import movies.{DisplayModel => DM, PhysicalModel => PM}
import StubScheduler.singlethreaded

class AnalyticsResourceSpec extends FreeSpec with Matchers with ResourceSpecTools {

  def setup () = {
    implicit val random = Random
    implicit val store = StubStore()
    val movies = new MovieStore
    val resource = new AnalyticsResource
    val mock = MockApp (resource.delegate)
    (movies, mock)
  }

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

    "GET /rdd/movies should respond Ok with no items" in {
      val (movies, mock) = setup()
      val response = mock.get ("/rdd/movies")
      response.code should be (Ok)
      response.body should matchJson ("[]")
    }

    "GET /rdd/actors should respond Ok with no items" in {
      val (movies, mock) = setup()
      val response = mock.get ("/rdd/actors")
      response.code should be (Ok)
      response.body should matchJson ("[]")
    }

    "GET /rdd/roles should respond Ok with no items" in {
      val (movies, mock) = setup()
      val response = mock.get ("/rdd/roles")
      response.code should be (Ok)
      response.body should matchJson ("[]")
    }}

  "When the database has a movie and actor" - {

    "GET /rdd/movies should respond Ok with no items" in {
      val (movies, mock) = setup()
      addMovieAndActor (movies)
      val response = mock.get ("/rdd/movies")
      response.code should be (Ok)
      response.body should matchJson ("""[ {
        "id" : "1",
        "title" : "Star Wars",
        "released" : null
      }]""")
    }

    "GET /rdd/actors should respond Ok with no items" in {
      val (movies, mock) = setup()
      addMovieAndActor (movies)
      val response = mock.get ("/rdd/actors")
      response.code should be (Ok)
      response.body should matchJson ("""[{
        "id" : "1",
        "name" : "Mark Hamill",
        "born" : null
      }]""")
    }

    "GET /rdd/roles should respond Ok with no items" in {
      val (movies, mock) = setup()
      addMovieAndActor (movies)
      val response = mock.get ("/rdd/roles")
      response.code should be (Ok)
      response.body should matchJson ("""[{
        "movieId" : "1",
        "actorId" : "1",
        "role" : "Luke Skywalker"
      }]""")
    }}}
