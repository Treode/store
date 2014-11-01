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

import com.treode.store.stubs.StubStore
import com.twitter.finagle.http.MediaType
import com.twitter.finatra.test.MockApp
import org.scalatest.{FreeSpec, Matchers}

import movies.{PhysicalModel => PM}

class ActorResourceSpec extends FreeSpec with Matchers with ResourceSpecTools {

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

  def setup () = {
    implicit val random = Random
    implicit val store = StubStore()
    val movies = new MovieStore
    val resource = new ActorResource (0x6F, movies)
    val mock = MockApp (resource.delegate)
    (store, mock)
  }

  def addMarkHamill (mock: MockApp) = {
    val response = mock.put (
          "/actor/1",
          headers = Map (ContentType -> MediaType.Json),
          body = markHamill)
    response.code should be (Ok)
    response
  }

  "When the database is empty" - {

    "GET /actor/1 should respond Not Found" in {
      val (store, mock) = setup()
      val response = mock.get ("/actor/1")
      response.code should be (NotFound)
    }

    "PUT /actor/1 should respond Ok with an etag" in {
      val (store, mock) = setup()
      val r1 = addMarkHamill (mock)

      val t1 = r1.etag
      store.expectCells (PM.MovieTable) ()
      store.expectCells (PM.CastTable) ()
      store.expectCells (PM.ActorTable) (
          ("1", t1, PO.markHamill))
      store.expectCells (PM.RolesTable) (
          ("1", t1, PM.Roles.empty))
      store.expectCells (PM.Index) (
          ("mark hamill", t1, PO.actors ("1")))
    }

    "POST /actor should respond Ok with an etag" in {
      val (store, mock) = setup()

      val r1 = mock.post (
          "/actor",
          headers = Map (ContentType -> MediaType.Json),
          body = markHamill)
      r1.code should be (Ok)

      val uri = r1.getHeader (Location)
      val id = uri.substring ("/actor/".length)
      val r2 = mock.get (uri)
      r2.etag should be (r1.etag)
      r2.body should matchJson (s"""{
        "id": "$id",
        "name": "Mark Hamill",
        "born": null,
        "roles": []
      }""")
    }

    "PUT /actor/1 without a title should respond Bad Requst" in {
      val (store, mock) = setup()
      val r1 = mock.put (
            "/actor/1",
            headers = Map (ContentType -> MediaType.Json),
            body = "{}")
      r1.code should be (BadRequest)

      store.expectCells (PM.MovieTable) ()
      store.expectCells (PM.CastTable) ()
      store.expectCells (PM.ActorTable) ()
      store.expectCells (PM.RolesTable) ()
      store.expectCells (PM.Index) ()
    }

    "GET /actor?q=mark should respond Okay" in {
      val (store, mock) = setup()
      val response = mock.get ("/actor?q=mark")
      response.code should equal (Ok)
      response.body should matchJson (s"""{"movies": [], "actors": []}""")
    }}

  "When the database has an actor" - {

    "GET /actor/1 should respond Ok" in {
      val (store, mock) = setup()
      val r1 = addMarkHamill (mock)
      val r2 = mock.get ("/actor/1")
      r2.code should be (Ok)
      r2.etag should be (r1.etag)
      r2.body should matchJson (markHamill)
    }

    "GET /actor/1 with If-Modified-Since:0 should respond Ok" in {
      val (store, mock) = setup()
      val r1 = addMarkHamill (mock)
      val r2 = mock.get (
          "/actor/1",
          headers = Map (IfModifiedSince -> "0"))
      r2.code should be (Ok)
      r2.etag should be (r1.etag)
      r2.body should matchJson (markHamill)
    }

    "GET /actor/1 with If-Modified-Since:(r1.etag) should respond Not Modified" in {
      val (store, mock) = setup()
      val r1 = addMarkHamill (mock)
      val response = mock.get (
          "/actor/1",
          headers = Map (IfModifiedSince -> r1.etag.toString))
      response.code should be (NotModified)
      response.body should be ("")
    }

    "GET /actor/1 with Last-Modification-Before:(r1.etag-1) should respond Not Found" in {
      val (store, mock) = setup()
      val r1 = addMarkHamill (mock)
      val response = mock.get (
          "/actor/1",
          headers = Map (LastModificationBefore -> (r1.etag-1).toString))
      response.code should be (NotFound)
    }

    "GET /actor/1 with Last-Modification-Before:(r1.etag) should respond Ok" in {
      val (store, mock) = setup()
      val r1 = addMarkHamill (mock)
      val r2 = mock.get (
          "/actor/1",
          headers = Map (LastModificationBefore -> r1.etag.toString))
      r2.code should be (Ok)
      r2.etag should be (r1.etag)
      r2.body should matchJson (markHamill)
    }

    "PUT /actor/1 with should respond Ok with an etag" in {
        val (store, mock) = setup()
      val r1 = addMarkHamill (mock)
      val r2 = mock.put (
          "/actor/1",
          headers = Map (ContentType -> MediaType.Json),
          body = markHammer)
      r2.code should be (Ok)
      val etag = r2.etag

      val (t1, t2) = (r1.etag, r2.etag)
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

    "PUT /actor/1 with a If-Unmodified-Since:1 should respond Ok with an etag" in {
      val (store, mock) = setup()
      val r1 = addMarkHamill (mock)
      val r2 = mock.put (
          "/actor/1",
          headers = Map (ContentType -> MediaType.Json, IfUnmodifiedSince -> r1.etag.toString),
          body = markHammer)
      r2.code should be (Ok)

      val (t1, t2) = (r1.etag, r2.etag)
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

    "PUT /actor/1 with a If-Unmodified-Since:0 should respond Precondition Failed" in {
      val (store, mock) = setup()
      val r1 = addMarkHamill (mock)
      val r2 = mock.put (
          "/actor/1",
          headers = Map (ContentType -> MediaType.Json, IfUnmodifiedSince -> "0"),
          body = markHammer)
      r2.code should be (PreconditionFailed)

      val t1 = r1.etag
      store.expectCells (PM.MovieTable) ()
      store.expectCells (PM.CastTable) ()
      store.expectCells (PM.ActorTable) (
          ("1", t1, PO.markHamill))
      store.expectCells (PM.RolesTable) (
          ("1", t1, PM.Roles.empty))
      store.expectCells (PM.Index) (
          ("mark hamill", t1, PO.actors ("1")))
    }

    "GET /actor?q=mark should respond Okay" in {
        val (store, mock) = setup()
      val r1 = addMarkHamill (mock)
      val response = mock.get ("/actor?q=mark")
      response.code should equal (Ok)
      response.body should matchJson (s"""{
        "movies": [],
        "actors": [{
          "id": "1",
          "name": "Mark Hamill",
          "born": null
        }]
      }""")
    }}}
