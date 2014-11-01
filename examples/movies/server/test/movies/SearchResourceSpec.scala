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

import movies.{PhysicalModel => PM}
import StubScheduler.singlethreaded

class SearchResourceSpec extends FreeSpec with Matchers with ResourceSpecTools {

  def setup () (implicit scheduler: StubScheduler) = {
    implicit val random = Random
    implicit val store = StubStore()
    val movies = new MovieStore
    val resource = new SearchResource (0x6F, movies)
    val mock = MockApp (resource.delegate)
    (random, store, movies, mock)
  }

  "When the database is empty" - {

    "GET /search?q=johnny should respond Okay" in
      singlethreaded { implicit scheduler =>
        implicit val (random, store, movies, mock) = setup()
        val response = mock.get ("/search?q=johnny")
        response.code should equal (Ok)
        response.body should matchJson (s"""{"movies": [], "actors": []}""")
      }

    "GET /search/?q=johnny should respond Okay" in
      singlethreaded { implicit scheduler =>
        implicit val (random, store, movies, mock) = setup()
        val response = mock.get ("/search/?q=johnny")
        response.code should equal (Ok)
        response.body should matchJson (s"""{"movies": [], "actors": []}""")
      }

    "GET /search/foo?q=johnny should respond Not Found" in
      singlethreaded { implicit scheduler =>
        implicit val (random, store, movies, mock) = setup()
        val response = mock.get ("/search/foo?q=johnny")
        response.code should equal (NotFound)
      }}

  "When the database has a movie and and actor" - {

    "GET /search?q=johnny should respond Okay" in
      singlethreaded { implicit scheduler =>
        implicit val (random, store, movies, mock) = setup()

        val (id1, t1) = addTitle (t0, "Johnny Bravo")
        val (id2, t2) = addTitle (t0, "Star Wars")
        val (id3, t3) = addName (t0, "Johnny Depp")
        val (id4, t4) = addName (t0, "Mark Hamill")
        val t = Seq (t1, t2, t3, t4) .max

        val response = mock.get ("/search?q=johnny")
        response.code should equal (Ok)
        response.body should matchJson (s"""{
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
        }""")
      }}}
