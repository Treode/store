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

class MovieResourceSpec extends FreeSpec with Matchers with ResourceSpecTools {

  val starWars = """{"id": "1", "title": "Star Wars", "released": null, "cast": []}"""
  val aNewHope = """{"id": "1", "title": "Star Wars: A New Hope", "released": null, "cast": []}"""

  def setup() = {
    implicit val random = new Random (0)
    implicit val scheduler = StubScheduler.random (random)
    implicit val store = StubStore()
    val movies = new MovieStore
    val resource = new MovieResource (0x6F, movies)
    val mock = MockApp (resource.delegate)
    (scheduler, store, mock)
  }

  def addStarWars (mock: MockApp) = {
    val response = mock.put (
          "/movie/1",
          headers = Map (ContentType -> MediaType.Json),
          body = starWars)
    response.code should equal (Ok)
    response
  }

  "When the database is empty" - {

    "GET /movie/1 should respond Not Found" in {
      implicit val (scheduler, store, mock) = setup()
      val response = mock.get ("/movie/1")
      response.code should equal (NotFound)
    }

    "PUT /movie/1 should respond Ok with an etag" in {
      implicit val (scheduler, store, mock) = setup()
      val r1 = addStarWars (mock)

      val t1 = r1.etag
      store.expectCells (PM.MovieTable) (
          ("1", t1, PO.starWars))
      store.expectCells (PM.MovieTitleIndex) (
          ("Star Wars", t1, Set ("1")))
      store.expectCells (PM.CastTable) (
          ("1", t1, PM.Cast.empty))
      store.expectCells (PM.ActorTable) ()
      store.expectCells (PM.ActorNameIndex) ()
      store.expectCells (PM.RolesTable) ()
    }

    "POST /movie should respond Ok with an etag" in {
      implicit val (scheduler, store, mock) = setup()

      val r1 = mock.post (
          "/movie",
          headers = Map (ContentType -> MediaType.Json),
          body = starWars)
      r1.code should equal (Ok)

      val uri = r1.getHeader (Location)
      val id = uri.substring ("/movie/".length)
      val r2 = mock.get (uri)
      r2.etag should be (r1.etag)
      r2.body should matchJson (s"""{"id":"$id","title":"Star Wars","released":null,"cast":[]}""")
    }

    "PUT /movie/1 without a title should respond Bad Requst" in {
      implicit val (scheduler, store, mock) = setup()
      val r1 = mock.put (
            "/movie/1",
            headers = Map (ContentType -> MediaType.Json),
            body = "{}")
      r1.code should equal (BadRequest)

      store.expectCells (PM.MovieTable) ()
      store.expectCells (PM.MovieTitleIndex) ()
      store.expectCells (PM.CastTable) ()
      store.expectCells (PM.ActorTable) ()
      store.expectCells (PM.ActorNameIndex) ()
      store.expectCells (PM.RolesTable) ()
    }}

  "When the database has a movie" - {

    "GET /movie/1 should respond Ok" in {
      val (scheduler, store, mock) = setup()
      val r1 = addStarWars (mock)
      val r2 = mock.get ("/movie/1")
      r2.code should equal (Ok)
      r2.etag should be (r1.etag)
      r2.body should matchJson (starWars)
    }

    "GET /movie/1 with If-Modified-Since:0 should respond Ok" in {
      val (scheduler, store, mock) = setup()
      val r1 = addStarWars (mock)
      val r2 = mock.get (
          "/movie/1",
          headers = Map (IfModifiedSince -> "0"))
      r2.code should equal (Ok)
      r2.etag should be (r1.etag)
      r2.body should matchJson (starWars)
    }

    "GET /movie/1 with If-Modified-Since:(r1.etag) should respond Not Modified" in {
      val (scheduler, store, mock) = setup()
      val r1 = addStarWars (mock)
      val response = mock.get (
          "/movie/1",
          headers = Map (IfModifiedSince -> r1.etag.toString))
      response.code should equal (NotModified)
      response.body should be ("")
    }

    "GET /movie/1 with Last-Modification-Before:(r1.etag-1) should respond Not Found" in {
      val (scheduler, store, mock) = setup()
      val r1 = addStarWars (mock)
      val response = mock.get (
          "/movie/1",
          headers = Map (LastModificationBefore -> (r1.etag-1).toString))
      response.code should equal (NotFound)
    }

    "GET /movie/1 with Last-Modification-Before:(r1.etag) should respond Ok" in {
      val (scheduler, store, mock) = setup()
      val r1 = addStarWars (mock)
      val r2 = mock.get (
          "/movie/1",
          headers = Map (LastModificationBefore -> r1.etag.toString))
      r2.code should equal (Ok)
      r2.etag should be (r1.etag)
      r2.body should matchJson (starWars)
    }

    "PUT /movie/1 with should respond Ok with an etag" in {
      val (scheduler, store, mock) = setup()
      val r1 = addStarWars (mock)
      val r2 = mock.put (
          "/movie/1",
          headers = Map (ContentType -> MediaType.Json),
          body = aNewHope)
      r2.code should equal (Ok)
      val etag = r2.etag

      val (t1, t2) = (r1.etag, r2.etag)
      store.expectCells (PM.MovieTable) (
          ("1", t2, PO.aNewHope),
          ("1", t1, PO.starWars))
      store.expectCells (PM.MovieTitleIndex) (
          ("Star Wars", t2, None),
          ("Star Wars", t1, Set ("1")),
          ("Star Wars: A New Hope", t2, Set ("1")))
      store.expectCells (PM.CastTable) (
          ("1", t1, PM.Cast.empty))
      store.expectCells (PM.ActorTable) ()
      store.expectCells (PM.ActorNameIndex) ()
      store.expectCells (PM.RolesTable) ()
    }

    "PUT /movie/1 with a If-Unmodified-Since:1 should respond Ok with an etag" in {
      val (scheduler, store, mock) = setup()
      val r1 = addStarWars (mock)
      val r2 = mock.put (
          "/movie/1",
          headers = Map (ContentType -> MediaType.Json, IfUnmodifiedSince -> r1.etag.toString),
          body = aNewHope)
      r2.code should equal (Ok)

      val (t1, t2) = (r1.etag, r2.etag)
      store.expectCells (PM.MovieTable) (
          ("1", t2, PO.aNewHope),
          ("1", t1, PO.starWars))
      store.expectCells (PM.MovieTitleIndex) (
          ("Star Wars", t2, None),
          ("Star Wars", t1, Set ("1")),
          ("Star Wars: A New Hope", t2, Set ("1")))
      store.expectCells (PM.CastTable) (
          ("1", t1, PM.Cast.empty))
      store.expectCells (PM.ActorTable) ()
      store.expectCells (PM.ActorNameIndex) ()
      store.expectCells (PM.RolesTable) ()
    }

    "PUT /movie/1 with a If-Unmodified-Since:0 should respond Precondition Failed" in {
      val (scheduler, store, mock) = setup()
      val r1 = addStarWars (mock)
      val r2 = mock.put (
          "/movie/1",
          headers = Map (ContentType -> MediaType.Json, IfUnmodifiedSince -> "0"),
          body = aNewHope)
      r2.code should equal (PreconditionFailed)

      val t1 = r1.etag
      store.expectCells (PM.MovieTable) (
          ("1", t1, PO.starWars))
      store.expectCells (PM.MovieTitleIndex) (
          ("Star Wars", t1, Set ("1")))
      store.expectCells (PM.CastTable) (
          ("1", t1, PM.Cast.empty))
      store.expectCells (PM.ActorTable) ()
      store.expectCells (PM.ActorNameIndex) ()
      store.expectCells (PM.RolesTable) ()
    }}}
