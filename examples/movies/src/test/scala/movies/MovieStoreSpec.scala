/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version "2".0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-"2".0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package movies

import scala.util.Random

import com.fasterxml.jackson.databind.JsonNode
import com.treode.finatra.BadRequestException
import com.treode.store.{Bytes, Cell, TxClock, TxId}
import com.treode.store.stubs.StubStore
import com.treode.async.Async
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import org.scalatest.FlatSpec

import movies.{DisplayModel => DM, PhysicalModel => PM}

class MovieStoreSpec extends FlatSpec with SpecTools {

  def setup() = {
    implicit val random = new Random (0)
    implicit val scheduler = StubScheduler.random()
    implicit val store = StubStore()
    val movies = new MovieStore
    (random, scheduler, store, movies)
  }

  def expectReadResult [A] (
      actual: Async [(TxClock, Option [A])]
  ) (
      time: TxClock, json: String
  ) (implicit
      s: StubScheduler,
      m: Manifest [A]
  ) {
    val (_t, _v) = actual.expectPass()
    assertResult (time) (_t)
    assertResult (json.fromJson [A]) (_v.get)
  }

  "The MovieStore" should "create a movie with no cast" in {
    implicit val (random, scheduler, store, movies) = setup()

    val (id, t1) = movies.create (random.nextXid, t0, """ {
        "title": "Star Wars"
    } """ .fromJson [DM.Movie]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (id, t1, PO.starWars))
    store.expectCells (PM.CastTable) (
        (id, t1, PM.Cast.empty))
    store.expectCells (PM.ActorTable) ()
    store.expectCells (PM.RolesTable) ()
    store.expectCells (PM.Index) (
        ("star wars", t1, PO.movies (id)))

    expectReadResult (movies.readMovie (t1, id)) (t1, s""" {
        "id": "$id",
        "title": "Star Wars",
        "cast": []
    } """)
  }

  it should "update a non-existent movie with no cast" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 = movies.update (random.nextXid, t0, "1", """ {
        "title": "Star Wars"
    } """ .fromJson [DM.Movie]) .expectPass()

    store.expectCells (PM.MovieTable) (
        ("1", t1, PO.starWars))
    store.expectCells (PM.CastTable) (
        ("1", t1, PM.Cast.empty))
    store.expectCells (PM.ActorTable) ()
    store.expectCells (PM.RolesTable) ()
    store.expectCells (PM.Index) (
        ("star wars", t1, PO.movies ("1")))

    expectReadResult (movies.readMovie (t1, "1")) (t1, s""" {
        "id": "1",
        "title": "Star Wars",
        "cast": []
    } """)
  }

  it should "create a movie with an empty cast" in {
    implicit val (random, scheduler, store, movies) = setup()

    val (id, t1) = movies.create (random.nextXid, t0, """ {
        "title": "Star Wars",
        "cast": []
    } """ .fromJson [DM.Movie]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (id, t1, PO.starWars))
    store.expectCells (PM.CastTable) (
        (id, t1, PM.Cast.empty))
    store.expectCells (PM.ActorTable) ()
    store.expectCells (PM.RolesTable) ()
    store.expectCells (PM.Index) (
        ("star wars", t1, PO.movies (id)))

    expectReadResult (movies.readMovie (t1, id)) (t1, s""" {
        "id": "$id",
        "title": "Star Wars",
        "cast": []
    } """)
  }

  it should "update a non-existent movie with an empty cast" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 = movies.update (random.nextXid, t0, "1", """ {
        "title": "Star Wars",
        "cast": []
    } """ .fromJson [DM.Movie]) .expectPass()

    store.expectCells (PM.MovieTable) (
        ("1", t1, PO.starWars))
    store.expectCells (PM.CastTable) (
        ("1", t1, PM.Cast.empty))
    store.expectCells (PM.ActorTable) ()
    store.expectCells (PM.RolesTable) ()
    store.expectCells (PM.Index) (
        ("star wars", t1, PO.movies ("1")))

    expectReadResult (movies.readMovie (t1, "1")) (t1, s""" {
        "id": "1",
        "title": "Star Wars",
        "cast": []
    } """)
  }

  it should "reject a movie with no title" in {
    implicit val (random, scheduler, store, movies) = setup()

    val exn = movies.create (random.nextXid, t0, """ {
        "cast": []
    } """ .fromJson [DM.Movie]) .fail [BadRequestException]
    println (exn)

    store.expectCells (PM.MovieTable) ()
    store.expectCells (PM.CastTable) ()
    store.expectCells (PM.ActorTable) ()
    store.expectCells (PM.RolesTable) ()
    store.expectCells (PM.Index) ()
  }

  it should "reject a cast member with no actorId" in {
    implicit val (random, scheduler, store, movies) = setup()

    movies.create (random.nextXid, t0, """ {
        "title": "Star Wars",
        "cast": [ { "actor": "Mark Hamill" } ]
    } """ .fromJson [DM.Movie]) .fail [BadRequestException]

    store.expectCells (PM.MovieTable) ()
    store.expectCells (PM.CastTable) ()
    store.expectCells (PM.ActorTable) ()
    store.expectCells (PM.RolesTable) ()
    store.expectCells (PM.Index) ()
  }

  it should "reject a cast member for an actor that does not exist" in {
    implicit val (random, scheduler, store, movies) = setup()

    movies.create (random.nextXid, t0, """ {
        "title": "Star Wars",
        "cast": [ { "actorId": "1" } ]
    } """ .fromJson [DM.Movie]) .fail [BadRequestException]

    store.expectCells (PM.MovieTable) ()
    store.expectCells (PM.CastTable) ()
    store.expectCells (PM.ActorTable) ()
    store.expectCells (PM.RolesTable) ()
    store.expectCells (PM.Index) ()
  }

  it should "create a movie with one cast member" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 = movies.update (random.nextXid, t0, "1", DO.markHamill) .expectPass()

    val (id, t2) = movies.create (random.nextXid, t1, """ {
        "title": "Star Wars",
        "cast": [ { "actorId": "1", "role": "Luke Skywalker" } ]
    } """ .fromJson [DM.Movie]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (id, t2, PO.starWars))
    store.expectCells (PM.CastTable) (
        (id, t2, PM.Cast (Seq (PM.CastMember ("1", "Luke Skywalker")))))
    store.expectCells (PM.ActorTable) (
        ("1", t1, PO.markHamill))
    store.expectCells (PM.RolesTable) (
        ("1", t2, PM.Roles (Seq (PM.Role (id, "Luke Skywalker")))),
        ("1", t1, PM.Roles.empty))
    store.expectCells (PM.Index) (
        ("mark hamill", t1, PO.actors ("1")),
        ("star wars", t2, PO.movies (id)))

    expectReadResult (movies.readMovie (t2, id)) (t2, s""" {
        "id": "$id",
        "title": "Star Wars",
        "cast": [ { "actorId": "1", "actor": "Mark Hamill", "role": "Luke Skywalker" } ]
    } """)
  }

  it should "create a movie with three cast members" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 = movies.update (random.nextXid, t0, "1", DO.markHamill) .expectPass()
    val t2 = movies.update (random.nextXid, t0, "2", DO.harrisonFord) .expectPass()
    val t3 = movies.update (random.nextXid, t0, "3", DO.carrieFisher) .expectPass()

    val (id, t4) = movies.create (random.nextXid, t3, """ {
        "title": "Star Wars",
        "cast": [
            { "actorId": "1", "role": "Luke Skywalker" },
            { "actorId": "2", "role": "Han Solo" },
            { "actorId": "3", "role": "Princess Leia Organa" }
        ]
    } """ .fromJson [DM.Movie]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (id, t4, PO.starWars))
    store.expectCells (PM.CastTable) (
        (id, t4, PM.Cast (Seq (
            PM.CastMember ("1", "Luke Skywalker"),
            PM.CastMember ("2", "Han Solo"),
            PM.CastMember ("3", "Princess Leia Organa")))))
    store.expectCells (PM.ActorTable) (
        ("1", t1, PO.markHamill),
        ("2", t2, PO.harrisonFord),
        ("3", t3, PO.carrieFisher))
    store.expectCells (PM.RolesTable) (
        ("1", t4, PM.Roles (Seq (PM.Role (id, "Luke Skywalker")))),
        ("1", t1, PM.Roles.empty),
        ("2", t4, PM.Roles (Seq (PM.Role (id, "Han Solo")))),
        ("2", t2, PM.Roles.empty),
        ("3", t4, PM.Roles (Seq (PM.Role (id, "Princess Leia Organa")))),
        ("3", t3, PM.Roles.empty))
    store.expectCells (PM.Index) (
        ("carrie fisher", t3, PO.actors ("3")),
        ("harrison ford", t2, PO.actors ("2")),
        ("mark hamill", t1, PO.actors ("1")),
        ("star wars", t4, PO.movies (id)))

    expectReadResult (movies.readMovie (t4, id)) (t4, s""" {
        "id": "$id",
        "title": "Star Wars",
        "cast": [
            { "actorId": "1", "actor": "Mark Hamill", "role": "Luke Skywalker" },
            { "actorId": "2", "actor": "Harrison Ford", "role": "Han Solo" },
            { "actorId": "3", "actor": "Carrie Fisher", "role": "Princess Leia Organa" }
        ]
    } """)
  }

  it should "update the movie's title" in {
    implicit val (random, scheduler, store, movies) = setup()

    val (id, t1) = movies.create (random.nextXid, t0, """ {
        "title": "Star Wars"
    } """ .fromJson [DM.Movie]) .expectPass()

    val t2 = movies.update (random.nextXid, t1, id, """ {
        "title": "Star Wars: A New Hope"
    } """ .fromJson [DM.Movie]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (id, t2, PO.aNewHope),
        (id, t1, PO.starWars))
    store.expectCells (PM.CastTable) (
        (id, t1, PM.Cast.empty))
    store.expectCells (PM.ActorTable) ()
    store.expectCells (PM.RolesTable) ()
    store.expectCells (PM.Index) (
        ("star wars", t2, None),
        ("star wars", t1, PO.movies (id)),
        ("star wars: a new hope", t2, PO.movies (id)))

    expectReadResult (movies.readMovie (t2, id)) (t2, s""" {
        "id": "$id",
        "title": "Star Wars: A New Hope",
        "cast": []
    } """)

    expectReadResult (movies.readMovie (t1, id)) (t1, s""" {
        "id": "$id",
        "title": "Star Wars",
        "cast": []
    } """)
  }

  it should "update the movie's release date" in {
    implicit val (random, scheduler, store, movies) = setup()

    val (id, t1) = movies.create (random.nextXid, t0, """ {
        "title": "Star Wars",
        "released": "1980-06-20"
    } """ .fromJson [DM.Movie]) .expectPass()

    val t2 = movies.update (random.nextXid, t1, id, """ {
        "released": "1977-05-25"
    } """ .fromJson [DM.Movie]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (id, t2, PM.Movie ("Star Wars", may_25_1977)),
        (id, t1, PM.Movie ("Star Wars", jun_20_1980)))
    store.expectCells (PM.CastTable) (
        (id, t1, PM.Cast.empty))
    store.expectCells (PM.ActorTable) ()
    store.expectCells (PM.RolesTable) ()
    store.expectCells (PM.Index) (
        ("star wars", t1, PO.movies (id)))


    expectReadResult (movies.readMovie (t2, id)) (t2, s""" {
        "id": "$id",
        "title": "Star Wars",
        "released": "1977-05-25",
        "cast": []
    } """)

    expectReadResult (movies.readMovie (t1, id)) (t1, s""" {
        "id": "$id",
        "title": "Star Wars",
        "released": "1980-06-20",
        "cast": []
    } """)
  }

  it should "update a cast member" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 = movies.update (random.nextXid, t0, "1", DO.markHamill) .expectPass()

    val (id, t2) = movies.create (random.nextXid, t1, """ {
        "title": "Star Wars",
        "cast": [ { "actorId": "1", "role": "Luke Skywriter" } ]
    } """ .fromJson [DM.Movie]) .expectPass()

    val t3 = movies.update (random.nextXid, t2, id, """ {
        "title": "Star Wars",
        "cast": [ { "actorId": "1", "role": "Luke Skywalker" } ]
    } """ .fromJson [DM.Movie]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (id, t2, PO.starWars))
    store.expectCells (PM.CastTable) (
        (id, t3, PM.Cast (Seq (PM.CastMember ("1", "Luke Skywalker")))),
        (id, t2, PM.Cast (Seq (PM.CastMember ("1", "Luke Skywriter")))))
    store.expectCells (PM.ActorTable) (
        ("1", t1, PO.markHamill))
    store.expectCells (PM.RolesTable) (
        ("1", t3, PM.Roles (Seq (PM.Role (id, "Luke Skywalker")))),
        ("1", t2, PM.Roles (Seq (PM.Role (id, "Luke Skywriter")))),
        ("1", t1, PM.Roles.empty))
    store.expectCells (PM.Index) (
        ("mark hamill", t1, PO.actors ("1")),
        ("star wars", t2, PO.movies (id)))

    expectReadResult (movies.readMovie (t2, id)) (t2, s""" {
        "id": "$id",
        "title": "Star Wars",
        "cast": [ { "actorId": "1", "actor": "Mark Hamill", "role": "Luke Skywriter" } ]
    } """)

    expectReadResult (movies.readMovie (t3, id)) (t3, s""" {
        "id": "$id",
        "title": "Star Wars",
        "cast": [ { "actorId": "1", "actor": "Mark Hamill", "role": "Luke Skywalker" } ]
    } """)
  }

  it should "add a cast member" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 = movies.update (random.nextXid, t0, "1", DO.markHamill) .expectPass()
    val t2 = movies.update (random.nextXid, t0, "2", DO.harrisonFord) .expectPass()
    val t3 = movies.update (random.nextXid, t0, "3", DO.carrieFisher) .expectPass()

    val (id, t4) = movies.create (random.nextXid, t3, """ {
        "title": "Star Wars",
        "cast": [
            { "actorId": "1", "role": "Luke Skywalker" },
            { "actorId": "3", "role": "Princess Leia Organa" }
        ]
    } """ .fromJson [DM.Movie]) .expectPass()

    val t5 = movies.update (random.nextXid, t4, id, """ {
        "title": "Star Wars",
        "cast": [
            { "actorId": "1", "role": "Luke Skywalker" },
            { "actorId": "2", "role": "Han Solo" },
            { "actorId": "3", "role": "Princess Leia Organa" }
        ]
    } """ .fromJson [DM.Movie]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (id, t4, PO.starWars))
    store.expectCells (PM.CastTable) (
        (id, t5, PM.Cast (Seq (
            PM.CastMember ("1", "Luke Skywalker"),
            PM.CastMember ("2", "Han Solo"),
            PM.CastMember ("3", "Princess Leia Organa")))),
        (id, t4, PM.Cast (Seq (
            PM.CastMember ("1", "Luke Skywalker"),
            PM.CastMember ("3", "Princess Leia Organa")))))
    store.expectCells (PM.ActorTable) (
        ("1", t1, PO.markHamill),
        ("2", t2, PO.harrisonFord),
        ("3", t3, PO.carrieFisher))
    store.expectCells (PM.RolesTable) (
        ("1", t4, PM.Roles (Seq (PM.Role (id, "Luke Skywalker")))),
        ("1", t1, PM.Roles.empty),
        ("2", t5, PM.Roles (Seq (PM.Role (id, "Han Solo")))),
        ("2", t2, PM.Roles.empty),
        ("3", t4, PM.Roles (Seq (PM.Role (id, "Princess Leia Organa")))),
        ("3", t3, PM.Roles.empty))
    store.expectCells (PM.Index) (
        ("carrie fisher", t3, PO.actors ("3")),
        ("harrison ford", t2, PO.actors ("2")),
        ("mark hamill", t1, PO.actors ("1")),
        ("star wars", t4, PO.movies (id)))

    expectReadResult (movies.readMovie (t4, id)) (t4, s""" {
        "id": "$id",
        "title": "Star Wars",
        "cast": [
            { "actorId": "1", "actor": "Mark Hamill", "role": "Luke Skywalker" },
            { "actorId": "3", "actor": "Carrie Fisher", "role": "Princess Leia Organa" }
        ]
    } """)

    expectReadResult (movies.readMovie (t5, id)) (t5, s""" {
        "id": "$id",
        "title": "Star Wars",
        "cast": [
            { "actorId": "1", "actor": "Mark Hamill", "role": "Luke Skywalker" },
            { "actorId": "2", "actor": "Harrison Ford", "role": "Han Solo" },
            { "actorId": "3", "actor": "Carrie Fisher", "role": "Princess Leia Organa" }
        ]
    } """)
  }

  it should "remove a cast member" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 = movies.update (random.nextXid, t0, "1", DO.markHamill) .expectPass()
    val t2 = movies.update (random.nextXid, t0, "2", DO.harrisonFord) .expectPass()
    val t3 = movies.update (random.nextXid, t0, "3", DO.carrieFisher) .expectPass()

    val (id, t4) = movies.create (random.nextXid, t3, """ {
        "title": "Star Wars",
        "cast": [
            { "actorId": "1", "role": "Luke Skywalker" },
            { "actorId": "2", "role": "Han Solo" },
            { "actorId": "3", "role": "Princess Leia Organa" }
        ]
    } """ .fromJson [DM.Movie]) .expectPass()

    val t5 = movies.update (random.nextXid, t4, id, """ {
        "title": "Star Wars",
        "cast": [
            { "actorId": "1", "role": "Luke Skywalker"},
            { "actorId": "3", "role": "Princess Leia Organa"}
        ]
    } """ .fromJson [DM.Movie]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (id, t4, PO.starWars))
    store.expectCells (PM.CastTable) (
        (id, t5, PM.Cast (Seq (
            PM.CastMember ("1", "Luke Skywalker"),
            PM.CastMember ("3", "Princess Leia Organa")))),
        (id, t4, PM.Cast (Seq (
            PM.CastMember ("1", "Luke Skywalker"),
            PM.CastMember ("2", "Han Solo"),
            PM.CastMember ("3", "Princess Leia Organa")))))
    store.expectCells (PM.ActorTable) (
        ("1", t1, PO.markHamill),
        ("2", t2, PO.harrisonFord),
        ("3", t3, PO.carrieFisher))
    store.expectCells (PM.RolesTable) (
        ("1", t4, PM.Roles (Seq (PM.Role (id, "Luke Skywalker")))),
        ("1", t1, PM.Roles.empty),
        ("2", t5, PM.Roles.empty),
        ("2", t4, PM.Roles (Seq (PM.Role (id, "Han Solo")))),
        ("2", t2, PM.Roles.empty),
        ("3", t4, PM.Roles (Seq (PM.Role (id, "Princess Leia Organa")))),
        ("3", t3, PM.Roles.empty))
    store.expectCells (PM.Index) (
        ("carrie fisher", t3, PO.actors ("3")),
        ("harrison ford", t2, PO.actors ("2")),
        ("mark hamill", t1, PO.actors ("1")),
        ("star wars", t4, PO.movies (id)))

    expectReadResult (movies.readMovie (t4, id)) (t4, s""" {
        "id": "$id",
        "title": "Star Wars",
        "cast":  [
            { "actorId": "1", "actor": "Mark Hamill", "role": "Luke Skywalker" },
            { "actorId": "2", "actor": "Harrison Ford", "role": "Han Solo" },
            { "actorId": "3", "actor": "Carrie Fisher", "role": "Princess Leia Organa" }
        ]
    } """)

    expectReadResult (movies.readMovie (t5, id)) (t5, s""" {
        "id": "$id",
        "title": "Star Wars",
        "cast":  [
            { "actorId": "1", "actor": "Mark Hamill", "role": "Luke Skywalker" },
            { "actorId": "3", "actor": "Carrie Fisher", "role": "Princess Leia Organa" }
        ]
    } """)
  }

  it should "replace a cast member" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 = movies.update (random.nextXid, t0, "1", DO.markHamill) .expectPass()
    val t2 = movies.update (random.nextXid, t0, "2", DO.harrisonFord) .expectPass()
    val t3 = movies.update (random.nextXid, t0, "3", DO.carrieFisher) .expectPass()

    val (id, t4) = movies.create (random.nextXid, t3, """ {
        "title": "Star Wars",
        "cast": [
            { "actorId": "1", "role": "Luke Skywalker" },
            { "actorId": "2", "role": "Han Solo" }
        ]
    } """ .fromJson [DM.Movie]) .expectPass()

    val t5 = movies.update (random.nextXid, t4, id, """ {
        "title": "Star Wars",
        "cast": [
            { "actorId": "1", "role": "Luke Skywalker" },
            { "actorId": "3", "role": "Princess Leia Organa" }
        ]
    } """ .fromJson [DM.Movie]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (id, t4, PO.starWars))
    store.expectCells (PM.CastTable) (
        (id, t5, PM.Cast (Seq (
            PM.CastMember ("1", "Luke Skywalker"),
            PM.CastMember ("3", "Princess Leia Organa")))),
        (id, t4, PM.Cast (Seq (
            PM.CastMember ("1", "Luke Skywalker"),
            PM.CastMember ("2", "Han Solo")))))
    store.expectCells (PM.ActorTable) (
        ("1", t1, PO.markHamill),
        ("2", t2, PO.harrisonFord),
        ("3", t3, PO.carrieFisher))
    store.expectCells (PM.RolesTable) (
        ("1", t4, PM.Roles (Seq (PM.Role (id, "Luke Skywalker")))),
        ("1", t1, PM.Roles.empty),
        ("2", t5, PM.Roles.empty),
        ("2", t4, PM.Roles (Seq (PM.Role (id, "Han Solo")))),
        ("2", t2, PM.Roles.empty),
        ("3", t5, PM.Roles (Seq (PM.Role (id, "Princess Leia Organa")))),
        ("3", t3, PM.Roles.empty))
    store.expectCells (PM.Index) (
        ("carrie fisher", t3, PO.actors ("3")),
        ("harrison ford", t2, PO.actors ("2")),
        ("mark hamill", t1, PO.actors ("1")),
        ("star wars", t4, PO.movies (id)))

    expectReadResult (movies.readMovie (t4, id)) (t4, s""" {
        "id": "$id",
        "title": "Star Wars",
        "cast":  [
            { "actorId": "1", "actor": "Mark Hamill", "role": "Luke Skywalker" },
            { "actorId": "2", "actor": "Harrison Ford", "role": "Han Solo" }
        ]
    } """)

    expectReadResult (movies.readMovie (t5, id)) (t5, s""" {
        "id": "$id",
        "title": "Star Wars",
        "cast":  [
            { "actorId": "1", "actor": "Mark Hamill", "role": "Luke Skywalker" },
            { "actorId": "3", "actor": "Carrie Fisher", "role": "Princess Leia Organa" }
        ]
    } """)
  }

  it should "ignore a movie update with no effect" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 = movies.update (random.nextXid, t0, "1", DO.markHamill) .expectPass()

    val (id, t2) = movies.create (random.nextXid, t1, """ {
        "title": "Star Wars",
        "cast": [ { "actorId": "1", "role": "Luke Skywalker" } ]
    } """ .fromJson [DM.Movie]) .expectPass()

    val t3 = movies.update (random.nextXid, t2, id, """ {
        "title": "Star Wars",
        "cast": [ { "actorId": "1", "role": "Luke Skywalker" } ]
    } """ .fromJson [DM.Movie]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (id, t2, PO.starWars))
    store.expectCells (PM.CastTable) (
        (id, t2, PM.Cast (Seq (PM.CastMember ("1", "Luke Skywalker")))))
    store.expectCells (PM.ActorTable) (
        ("1", t1, PO.markHamill))
    store.expectCells (PM.RolesTable) (
        ("1", t2, PM.Roles (Seq (PM.Role (id, "Luke Skywalker")))),
        ("1", t1, PM.Roles.empty))
    store.expectCells (PM.Index) (
        ("mark hamill", t1, PO.actors ("1")),
        ("star wars", t2, PO.movies (id)))

    expectReadResult (movies.readMovie (t2, id)) (t2, s""" {
        "id": "$id",
        "title": "Star Wars",
        "cast":  [ { "actorId": "1", "actor": "Mark Hamill", "role": "Luke Skywalker" } ]
    } """)

    expectReadResult (movies.readMovie (t3, id)) (t2, s""" {
        "id": "$id",
        "title": "Star Wars",
        "cast":  [ { "actorId": "1", "actor": "Mark Hamill", "role": "Luke Skywalker" } ]
    } """)
  }

  it should "leave the title unchanged when a movie update is missing it" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 = movies.update (random.nextXid, t0, "1", DO.markHamill) .expectPass()

    val (id, t2) = movies.create (random.nextXid, t1, """ {
        "title": "Star Wars",
        "cast": [ { "actorId": "1", "role": "Luke Skywalker" } ]
    } """ .fromJson [DM.Movie]) .expectPass()

    val t3 = movies.update (random.nextXid, t2, id, """ {
        "cast": [ { "actorId": "1", "role": "Luke Skywalker" } ]
    } """ .fromJson [DM.Movie]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (id, t2, PO.starWars))
    store.expectCells (PM.CastTable) (
        (id, t2, PM.Cast (Seq (PM.CastMember ("1", "Luke Skywalker")))))
    store.expectCells (PM.ActorTable) (
        ("1", t1, PO.markHamill))
    store.expectCells (PM.RolesTable) (
        ("1", t2, PM.Roles (Seq (PM.Role (id, "Luke Skywalker")))),
        ("1", t1, PM.Roles.empty))
    store.expectCells (PM.Index) (
        ("mark hamill", t1, PO.actors ("1")),
        ("star wars", t2, PO.movies (id)))

    expectReadResult (movies.readMovie (t3, id)) (t2, s""" {
        "id": "$id",
        "title": "Star Wars",
        "cast": [ { "actorId": "1", "actor": "Mark Hamill", "role": "Luke Skywalker" } ]
    } """)
  }

  it should "leave the cast unchanged when a movie update is missing it" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 = movies.update (random.nextXid, t0, "1", DO.markHamill) .expectPass()

    val (id, t2) = movies.create (random.nextXid, t1, """ {
        "title": "Star Wars",
        "cast": [ { "actorId": "1", "role": "Luke Skywalker" } ]
    } """ .fromJson [DM.Movie]) .expectPass()

    val t3 = movies.update (random.nextXid, t2, id, """ {
        "title": "Star Wars"
    } """ .fromJson [DM.Movie]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (id, t2, PO.starWars))
    store.expectCells (PM.CastTable) (
        (id, t2, PM.Cast (Seq (PM.CastMember ("1", "Luke Skywalker")))))
    store.expectCells (PM.ActorTable) (
        ("1", t1, PO.markHamill))
    store.expectCells (PM.RolesTable) (
        ("1", t2, PM.Roles (Seq (PM.Role (id, "Luke Skywalker")))),
        ("1", t1, PM.Roles.empty))
    store.expectCells (PM.Index) (
        ("mark hamill", t1, PO.actors ("1")),
        ("star wars", t2, PO.movies (id)))

    expectReadResult (movies.readMovie (t3, id)) (t2, s""" {
        "id": "$id",
        "title": "Star Wars",
        "cast": [ { "actorId": "1", "actor": "Mark Hamill", "role": "Luke Skywalker" } ]
    } """)
  }

  it should "leave the role name unchanged when a movie update is missing it" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 = movies.update (random.nextXid, t0, "1", DO.markHamill) .expectPass()

    val (id, t2) = movies.create (random.nextXid, t1, """ {
        "title": "Star Wars",
        "cast": [ { "actorId": "1", "role": "Luke Skywalker" } ]
    } """ .fromJson [DM.Movie]) .expectPass()

    val t3 = movies.update (random.nextXid, t2, id, """ {
        "title": "Star Wars",
        "cast": [ { "actorId": "1" } ]
    } """ .fromJson [DM.Movie]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (id, t2, PO.starWars))
    store.expectCells (PM.CastTable) (
        (id, t2, PM.Cast (Seq (PM.CastMember ("1", "Luke Skywalker")))))
    store.expectCells (PM.ActorTable) (
        ("1", t1, PO.markHamill))
    store.expectCells (PM.RolesTable) (
        ("1", t2, PM.Roles (Seq (PM.Role (id, "Luke Skywalker")))),
        ("1", t1, PM.Roles.empty))
    store.expectCells (PM.Index) (
        ("mark hamill", t1, PO.actors ("1")),
        ("star wars", t2, PO.movies (id)))

    expectReadResult (movies.readMovie (t3, id)) (t2, s""" {
        "id": "$id",
        "title": "Star Wars",
        "cast": [ { "actorId": "1", "actor": "Mark Hamill", "role": "Luke Skywalker" } ]
    } """)
  }

  it should "create two movies with the same title" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 = movies.update (random.nextXid, t0, "1", """ {
        "title": "The Piano"
    } """ .fromJson [DM.Movie]) .expectPass()

    val t2 = movies.update (random.nextXid, t1, "2", """ {
        "title": "The Piano"
    } """ .fromJson [DM.Movie]) .expectPass()

    store.expectCells (PM.MovieTable) (
        ("1", t1, PO.thePiano),
        ("2", t2, PO.thePiano))
    store.expectCells (PM.CastTable) (
        ("1", t1, PM.Cast.empty),
        ("2", t2, PM.Cast.empty))
    store.expectCells (PM.ActorTable) ()
    store.expectCells (PM.RolesTable) ()
    store.expectCells (PM.Index) (
        ("the piano", t2, PO.movies ("1", "2")),
        ("the piano", t1, PO.movies ("1")))
  }

  it should "create an actor with no roles" in {
    implicit val (random, scheduler, store, movies) = setup()

    val (id, t1) = movies.create (random.nextXid, t0, """ {
        "name": "Mark Hamill"
    } """ .fromJson [DM.Actor]) .expectPass()

    store.expectCells (PM.MovieTable) ()
    store.expectCells (PM.CastTable) ()
    store.expectCells (PM.ActorTable) (
        (id, t1, PO.markHamill))
    store.expectCells (PM.RolesTable) (
        (id, t1, PM.Roles.empty))
    store.expectCells (PM.Index) (
        ("mark hamill", t1, PO.actors (id)))

    expectReadResult (movies.readActor (t1, id)) (t1, s""" {
        "id": "$id",
        "name": "Mark Hamill",
        "roles":  []
    } """)
  }

  it should "update a non-existent actor with no roles" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 = movies.update (random.nextXid, t0, "1", """ {
        "name": "Mark Hamill"
    } """ .fromJson [DM.Actor]) .expectPass()

    store.expectCells (PM.MovieTable) ()
    store.expectCells (PM.CastTable) ()
    store.expectCells (PM.ActorTable) (
        ("1", t1, PO.markHamill))
    store.expectCells (PM.RolesTable) (
        ("1", t1, PM.Roles.empty))
    store.expectCells (PM.Index) (
        ("mark hamill", t1, PO.actors ("1")))

    expectReadResult (movies.readActor (t1, "1")) (t1, s""" {
        "id": "1",
        "name": "Mark Hamill",
        "roles":  []
    } """)
  }

  it should "create an actor with empty roles" in {
    implicit val (random, scheduler, store, movies) = setup()

    val (id, t1) = movies.create (random.nextXid, t0, """ {
        "name": "Mark Hamill",
        "roles": []
    } """ .fromJson [DM.Actor]) .expectPass()

    store.expectCells (PM.MovieTable) ()
    store.expectCells (PM.CastTable) ()
    store.expectCells (PM.ActorTable) (
        (id, t1, PO.markHamill))
    store.expectCells (PM.RolesTable) (
        (id, t1, PM.Roles.empty))
    store.expectCells (PM.Index) (
        ("mark hamill", t1, PO.actors (id)))

    expectReadResult (movies.readActor (t1, id)) (t1, s""" {
        "id": "$id",
        "name": "Mark Hamill",
        "roles":  []
    } """)
  }

  it should "update a non-existent actor with empty roles" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 = movies.update (random.nextXid, t0, "1", """ {
        "name": "Mark Hamill",
        "roles": []
    } """ .fromJson [DM.Actor]) .expectPass()

    store.expectCells (PM.MovieTable) ()
    store.expectCells (PM.CastTable) ()
    store.expectCells (PM.ActorTable) (
        ("1", t1, PO.markHamill))
    store.expectCells (PM.RolesTable) (
        ("1", t1, PM.Roles.empty))
    store.expectCells (PM.Index) (
        ("mark hamill", t1, PO.actors ("1")))

    expectReadResult (movies.readActor (t1, "1")) (t1, s""" {
        "id": "1",
        "name": "Mark Hamill",
        "roles":  []
    } """)
  }

  it should "reject a role with no movieId" in {
    implicit val (random, scheduler, store, movies) = setup()

    movies.create (random.nextXid, t0, """ {
        "name": "Mark Hamill",
        "roles": [ { "title": "Star Wars" } ]
    } """ .fromJson [DM.Actor]) .fail [BadRequestException]

    store.expectCells (PM.MovieTable) ()
    store.expectCells (PM.CastTable) ()
    store.expectCells (PM.ActorTable) ()
    store.expectCells (PM.RolesTable) ()
    store.expectCells (PM.Index) ()
  }

  it should "reject a role for a movie that does not exist" in {
    implicit val (random, scheduler, store, movies) = setup()

    movies.create (random.nextXid, t0, """ {
        "name": "Mark Hamill",
        "roles": [ { "movieId": "1" } ]
    } """ .fromJson [DM.Actor]) .fail [BadRequestException]

    store.expectCells (PM.MovieTable) ()
    store.expectCells (PM.CastTable) ()
    store.expectCells (PM.ActorTable) ()
    store.expectCells (PM.RolesTable) ()
    store.expectCells (PM.Index) ()
  }

  it should "create an actor with one role" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 = movies.update (random.nextXid, t0, "1", DO.starWars) .expectPass()

    val (id, t2) = movies.create (random.nextXid, t1, """ {
        "name": "Mark Hamill",
        "roles": [ { "movieId": "1", "role": "Luke Skywalker" } ]
    } """ .fromJson [DM.Actor]) .expectPass()

    store.expectCells (PM.MovieTable) (
        ("1", t1, PO.starWars))
    store.expectCells (PM.CastTable) (
        ("1", t2, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        ("1", t1, PM.Cast.empty))
    store.expectCells (PM.ActorTable) (
        (id, t2, PO.markHamill))
    store.expectCells (PM.RolesTable) (
        (id, t2, PM.Roles (Seq (PM.Role ("1", "Luke Skywalker")))))
    store.expectCells (PM.Index) (
        ("mark hamill", t2, PO.actors (id)),
        ("star wars", t1, PO.movies ("1")))

    expectReadResult (movies.readActor (t2, id)) (t2, s""" {
        "id": "$id",
        "name": "Mark Hamill",
        "roles":  [ { "movieId": "1", "title": "Star Wars", "role": "Luke Skywalker" } ]
    } """)
  }

  it should "create an actor with three roles" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 = movies.update (random.nextXid, t0, "1", DO.starWars) .expectPass()
    val t2 = movies.update (random.nextXid, t0, "2", DO.empireStrikesBack) .expectPass()
    val t3 = movies.update (random.nextXid, t0, "3", DO.returnOfTheJedi) .expectPass()

    val (id, t4) = movies.create (random.nextXid, t3, """ {
        "name": "Mark Hamill",
        "roles": [
            { "movieId": "1", "role": "Luke Skywalker" },
            { "movieId": "2", "role": "Luke Skywalker" },
            { "movieId": "3", "role": "Luke Skywalker" }
        ]
    } """ .fromJson [DM.Actor]) .expectPass()

    store.expectCells (PM.MovieTable) (
        ("1", t1, PO.starWars),
        ("2", t2, PO.empireStrikesBack),
        ("3", t3, PO.returnOfTheJedi))
    store.expectCells (PM.CastTable) (
        ("1", t4, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        ("1", t1, PM.Cast.empty),
        ("2", t4, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        ("2", t2, PM.Cast.empty),
        ("3", t4, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        ("3", t3, PM.Cast.empty))
    store.expectCells (PM.ActorTable) (
        (id, t4, PO.markHamill))
    store.expectCells (PM.RolesTable) (
        (id, t4, PM.Roles (Seq (
            PM.Role ("1", "Luke Skywalker"),
            PM.Role ("2", "Luke Skywalker"),
            PM.Role ("3", "Luke Skywalker")))))
    store.expectCells (PM.Index) (
        ("mark hamill", t4, PO.actors (id)),
        ("star wars", t1, PO.movies ("1")),
        ("star wars: return of the jedi", t3, PO.movies ("3")),
        ("star wars: the empire strikes back", t2, PO.movies ("2")))

    expectReadResult (movies.readActor (t4, id)) (t4, s""" {
        "id": "$id",
        "name": "Mark Hamill",
        "roles":  [
            { "movieId": "1", "title": "Star Wars", "role": "Luke Skywalker" },
            { "movieId": "2", "title": "Star Wars: The Empire Strikes Back", "role": "Luke Skywalker" },
            { "movieId": "3", "title": "Star Wars: Return of the Jedi", "role": "Luke Skywalker" }
        ]
    } """)
  }

  it should "update the actor's name" in {
    implicit val (random, scheduler, store, movies) = setup()

    val (id, t1) = movies.create (random.nextXid, t0, """ {
        "name": "Mark Hammer"
    } """ .fromJson [DM.Actor]) .expectPass()

    val t2 = movies.update (random.nextXid, t1, id, """ {
        "name": "Mark Hamill"
    } """ .fromJson [DM.Actor]) .expectPass()

    store.expectCells (PM.MovieTable) ()
    store.expectCells (PM.CastTable) ()
    store.expectCells (PM.ActorTable) (
        (id, t2, PO.markHamill),
        (id, t1, PO.markHammer))
    store.expectCells (PM.RolesTable) (
        (id, t1, PM.Roles.empty))
    store.expectCells (PM.Index) (
        ("mark hamill", t2, PO.actors (id)),
        ("mark hammer", t2, None),
        ("mark hammer", t1, PO.actors (id)))

    expectReadResult (movies.readActor (t1, id)) (t1, s""" {
        "id": "$id",
        "name": "Mark Hammer",
        "roles":  []
    } """)

    expectReadResult (movies.readActor (t2, id)) (t2, s""" {
        "id": "$id",
        "name": "Mark Hamill",
        "roles":  []
    } """)
  }

  it should "update the actor's birth date" in {
    implicit val (random, scheduler, store, movies) = setup()

    val (id, t1) = movies.create (random.nextXid, t0, """ {
        "name": "Mark Hamill",
        "born": "1977-05-25"
    } """ .fromJson [DM.Actor]) .expectPass()

    val t2 = movies.update (random.nextXid, t1, id, """ {
        "born": "1951-09-25"
    } """ .fromJson [DM.Actor]) .expectPass()

    store.expectCells (PM.MovieTable) ()
    store.expectCells (PM.CastTable) ()
    store.expectCells (PM.ActorTable) (
        (id, t2, PM.Actor ("Mark Hamill", sep_25_1951)),
        (id, t1, PM.Actor ("Mark Hamill", may_25_1977)))
    store.expectCells (PM.RolesTable) (
        (id, t1, PM.Roles.empty))
    store.expectCells (PM.Index) (
        ("mark hamill", t1, PO.actors (id)))

    expectReadResult (movies.readActor (t2, id)) (t2, s""" {
        "id": "$id",
        "name": "Mark Hamill",
        "born": "1951-09-25",
        "roles":  []
    } """)

    expectReadResult (movies.readActor (t1, id)) (t1, s""" {
        "id": "$id",
        "name": "Mark Hamill",
        "born": "1977-05-25",
        "roles":  []
    } """)
  }

  it should "update a role" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 = movies.update (random.nextXid, t0, "1", DO.starWars) .expectPass()

    val (id, t2) = movies.create (random.nextXid, t1, """ {
        "name": "Mark Hamill",
        "roles": [ { "movieId": "1", "role": "Luke Skywriter" } ]
    } """ .fromJson [DM.Actor]) .expectPass()

    val t3 = movies.update (random.nextXid, t2, id, """ {
        "name": "Mark Hamill",
        "roles": [ { "movieId": "1", "role": "Luke Skywalker" } ]
    } """ .fromJson [DM.Actor]) .expectPass()

    store.expectCells (PM.MovieTable) (
        ("1", t1, PO.starWars))
    store.expectCells (PM.CastTable) (
        ("1", t3, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        ("1", t2, PM.Cast (Seq (PM.CastMember (id, "Luke Skywriter")))),
        ("1", t1, PM.Cast.empty))
    store.expectCells (PM.ActorTable) (
        (id, t2, PO.markHamill))
    store.expectCells (PM.RolesTable) (
        (id, t3, PM.Roles (Seq (PM.Role ("1", "Luke Skywalker")))),
        (id, t2, PM.Roles (Seq (PM.Role ("1", "Luke Skywriter")))))
    store.expectCells (PM.Index) (
        ("mark hamill", t2, PO.actors (id)),
        ("star wars", t1, PO.movies ("1")))

    expectReadResult (movies.readActor (t2, id)) (t2, s""" {
        "id": "$id",
        "name": "Mark Hamill",
        "roles":  [ { "movieId": "1", "title": "Star Wars", "role": "Luke Skywriter" } ]
    } """)

    expectReadResult (movies.readActor (t3, id)) (t3, s""" {
        "id": "$id",
        "name": "Mark Hamill",
        "roles":  [ { "movieId": "1", "title": "Star Wars", "role": "Luke Skywalker" } ]
    } """)
  }

  it should "add a role" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 = movies.update (random.nextXid, t0, "1", DO.starWars) .expectPass()
    val t2 = movies.update (random.nextXid, t0, "2", DO.empireStrikesBack) .expectPass()
    val t3 = movies.update (random.nextXid, t0, "3", DO.returnOfTheJedi) .expectPass()

    val (id, t4) = movies.create (random.nextXid, t3, """ {
        "name": "Mark Hamill",
        "roles": [
            { "movieId": "1", "role": "Luke Skywalker" },
            { "movieId": "3", "role": "Luke Skywalker" }
        ]
    } """ .fromJson [DM.Actor]) .expectPass()

    val t5 = movies.update (random.nextXid, t4, id, """ {
        "name": "Mark Hamill",
        "roles": [
            { "movieId": "1", "role": "Luke Skywalker" },
            { "movieId": "2", "role": "Luke Skywalker" },
            { "movieId": "3", "role": "Luke Skywalker" }
        ]
    } """ .fromJson [DM.Actor]) .expectPass()

    store.expectCells (PM.MovieTable) (
        ("1", t1, PO.starWars),
        ("2", t2, PO.empireStrikesBack),
        ("3", t3, PO.returnOfTheJedi))
    store.expectCells (PM.CastTable) (
        ("1", t4, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        ("1", t1, PM.Cast.empty),
        ("2", t5, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        ("2", t2, PM.Cast.empty),
        ("3", t4, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        ("3", t3, PM.Cast.empty))
    store.expectCells (PM.ActorTable) (
        (id, t4, PO.markHamill))
    store.expectCells (PM.RolesTable) (
        (id, t5, PM.Roles (Seq (
            PM.Role ("1", "Luke Skywalker"),
            PM.Role ("2", "Luke Skywalker"),
            PM.Role ("3", "Luke Skywalker")))),
        (id, t4, PM.Roles (Seq (
            PM.Role ("1", "Luke Skywalker"),
            PM.Role ("3", "Luke Skywalker")))))
    store.expectCells (PM.Index) (
        ("mark hamill", t4, PO.actors (id)),
        ("star wars", t1, PO.movies ("1")),
        ("star wars: return of the jedi", t3, PO.movies ("3")),
        ("star wars: the empire strikes back", t2, PO.movies ("2")))

    expectReadResult (movies.readActor (t4, id)) (t4, s""" {
        "id": "$id",
        "name": "Mark Hamill",
        "roles":  [
            { "movieId": "1", "title": "Star Wars", "role": "Luke Skywalker" },
            { "movieId": "3", "title": "Star Wars: Return of the Jedi", "role": "Luke Skywalker" }
        ]
    } """)

    expectReadResult (movies.readActor (t5, id)) (t5, s""" {
        "id": "$id",
        "name": "Mark Hamill",
        "roles":  [
            { "movieId": "1", "title": "Star Wars", "role": "Luke Skywalker" },
            { "movieId": "2", "title": "Star Wars: The Empire Strikes Back", "role": "Luke Skywalker" },
            { "movieId": "3", "title": "Star Wars: Return of the Jedi", "role": "Luke Skywalker" }
        ]
    } """)
  }

  it should "remove a role" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 = movies.update (random.nextXid, t0, "1", DO.starWars) .expectPass()
    val t2 = movies.update (random.nextXid, t0, "2", DO.empireStrikesBack) .expectPass()
    val t3 = movies.update (random.nextXid, t0, "3", DO.returnOfTheJedi) .expectPass()

    val (id, t4) = movies.create (random.nextXid, t3, """ {
        "name": "Mark Hamill",
        "roles": [
            { "movieId": "1", "role": "Luke Skywalker" },
            { "movieId": "2", "role": "Luke Skywalker" },
            { "movieId": "3", "role": "Luke Skywalker" }
        ]
    } """ .fromJson [DM.Actor]) .expectPass()

    val t5 = movies.update (random.nextXid, t4, id, """ {
        "name": "Mark Hamill",
        "roles": [
            { "movieId": "1", "role": "Luke Skywalker" },
            { "movieId": "3", "role": "Luke Skywalker" }
        ]
    } """ .fromJson [DM.Actor]) .expectPass()

    store.expectCells (PM.MovieTable) (
        ("1", t1, PO.starWars),
        ("2", t2, PO.empireStrikesBack),
        ("3", t3, PO.returnOfTheJedi))
    store.expectCells (PM.CastTable) (
        ("1", t4, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        ("1", t1, PM.Cast.empty),
        ("2", t5, PM.Cast.empty),
        ("2", t4, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        ("2", t2, PM.Cast.empty),
        ("3", t4, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        ("3", t3, PM.Cast.empty))
    store.expectCells (PM.ActorTable) (
        (id, t4, PO.markHamill))
    store.expectCells (PM.RolesTable) (
        (id, t5, PM.Roles (Seq (
            PM.Role ("1", "Luke Skywalker"),
            PM.Role ("3", "Luke Skywalker")))),
        (id, t4, PM.Roles (Seq (
            PM.Role ("1", "Luke Skywalker"),
            PM.Role ("2", "Luke Skywalker"),
            PM.Role ("3", "Luke Skywalker")))))
    store.expectCells (PM.Index) (
        ("mark hamill", t4, PO.actors (id)),
        ("star wars", t1, PO.movies ("1")),
        ("star wars: return of the jedi", t3, PO.movies ("3")),
        ("star wars: the empire strikes back", t2, PO.movies ("2")))

    expectReadResult (movies.readActor (t4, id)) (t4, s""" {
        "id": "$id",
        "name": "Mark Hamill",
        "roles":  [
            { "movieId": "1", "title": "Star Wars", "role": "Luke Skywalker" },
            { "movieId": "2", "title": "Star Wars: The Empire Strikes Back", "role": "Luke Skywalker" },
            { "movieId": "3", "title": "Star Wars: Return of the Jedi", "role": "Luke Skywalker" }
        ]
    } """)

    expectReadResult (movies.readActor (t5, id)) (t5, s""" {
        "id": "$id",
        "name": "Mark Hamill",
        "roles":  [
            { "movieId": "1", "title": "Star Wars", "role": "Luke Skywalker" },
            { "movieId": "3", "title": "Star Wars: Return of the Jedi", "role": "Luke Skywalker" }
        ]
    } """)
  }

  it should "replace a role" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 = movies.update (random.nextXid, t0, "1", DO.starWars) .expectPass()
    val t2 = movies.update (random.nextXid, t0, "2", DO.empireStrikesBack) .expectPass()
    val t3 = movies.update (random.nextXid, t0, "3", DO.returnOfTheJedi) .expectPass()

    val (id, t4) = movies.create (random.nextXid, t3, """ {
        "name": "Mark Hamill",
        "roles": [
            { "movieId": "1", "role": "Luke Skywalker" },
            { "movieId": "2", "role": "Luke Skywalker" }
        ]
    } """ .fromJson [DM.Actor]) .expectPass()

    val t5 = movies.update (random.nextXid, t4, id, """ {
        "name": "Mark Hamill",
        "roles": [
            { "movieId": "1", "role": "Luke Skywalker" },
            { "movieId": "3", "role": "Luke Skywalker" }
        ]
    } """ .fromJson [DM.Actor]) .expectPass()

    store.expectCells (PM.MovieTable) (
        ("1", t1, PO.starWars),
        ("2", t2, PO.empireStrikesBack),
        ("3", t3, PO.returnOfTheJedi))
    store.expectCells (PM.CastTable) (
        ("1", t4, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        ("1", t1, PM.Cast.empty),
        ("2", t5, PM.Cast.empty),
        ("2", t4, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        ("2", t2, PM.Cast.empty),
        ("3", t5, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        ("3", t3, PM.Cast.empty))
    store.expectCells (PM.ActorTable) (
        (id, t4, PO.markHamill))
    store.expectCells (PM.RolesTable) (
        (id, t5, PM.Roles (Seq (
            PM.Role ("1", "Luke Skywalker"),
            PM.Role ("3", "Luke Skywalker")))),
        (id, t4, PM.Roles (Seq (
            PM.Role ("1", "Luke Skywalker"),
            PM.Role ("2", "Luke Skywalker")))))
    store.expectCells (PM.Index) (
        ("mark hamill", t4, PO.actors (id)),
        ("star wars", t1, PO.movies ("1")),
        ("star wars: return of the jedi", t3, PO.movies ("3")),
        ("star wars: the empire strikes back", t2, PO.movies ("2")))

    expectReadResult (movies.readActor (t4, id)) (t4, s""" {
        "id": "$id",
        "name": "Mark Hamill",
        "roles":  [
            { "movieId": "1", "title": "Star Wars", "role": "Luke Skywalker" },
            { "movieId": "2", "title": "Star Wars: The Empire Strikes Back", "role": "Luke Skywalker" }
        ]
    } """)

    expectReadResult (movies.readActor (t5, id)) (t5, s""" {
        "id": "$id",
        "name": "Mark Hamill",
        "roles":  [
            { "movieId": "1", "title": "Star Wars", "role": "Luke Skywalker" },
            { "movieId": "3", "title": "Star Wars: Return of the Jedi", "role": "Luke Skywalker" }
        ]
    } """)
  }

  it should "ignore an actor update with no effect" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 = movies.update (random.nextXid, t0, "1", DO.starWars) .expectPass()

    val (id, t2) = movies.create (random.nextXid, t1, """ {
        "name": "Mark Hamill",
        "roles": [ { "movieId": "1", "role": "Luke Skywalker" } ]
    } """ .fromJson [DM.Actor]) .expectPass()

    val t3 = movies.update (random.nextXid, t2, id, """ {
        "name": "Mark Hamill",
        "roles": [ { "movieId": "1", "role": "Luke Skywalker" } ]
    } """ .fromJson [DM.Actor]) .expectPass()

    store.expectCells (PM.MovieTable) (
        ("1", t1, PO.starWars))
    store.expectCells (PM.CastTable) (
        ("1", t2, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        ("1", t1, PM.Cast.empty))
    store.expectCells (PM.ActorTable) (
        (id, t2, PO.markHamill))
    store.expectCells (PM.RolesTable) (
        (id, t2, PM.Roles (Seq (PM.Role ("1", "Luke Skywalker")))))
    store.expectCells (PM.Index) (
        ("mark hamill", t2, PO.actors (id)),
        ("star wars", t1, PO.movies ("1")))

    expectReadResult (movies.readActor (t2, id)) (t2, s""" {
        "id": "$id",
        "name": "Mark Hamill",
        "roles":  [ { "movieId": "1", "title": "Star Wars", "role": "Luke Skywalker" } ]
    } """)

    expectReadResult (movies.readActor (t3, id)) (t2, s""" {
        "id": "$id",
        "name": "Mark Hamill",
        "roles":  [ { "movieId": "1", "title": "Star Wars", "role": "Luke Skywalker" } ]
    } """)
  }

  it should "leave the name unchanged when an actor update is missing it" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 = movies.update (random.nextXid, t0, "1", DO.starWars) .expectPass()

    val (id, t2) = movies.create (random.nextXid, t1, """ {
        "name": "Mark Hamill",
        "roles": [ { "movieId": "1", "role": "Luke Skywalker" } ]
    } """ .fromJson [DM.Actor]) .expectPass()

    val t3 = movies.update (random.nextXid, t2, id, """ {
        "roles": [ { "movieId": "1", "role": "Luke Skywalker" } ]
    } """ .fromJson [DM.Actor]) .expectPass()

    store.expectCells (PM.MovieTable) (
        ("1", t1, PO.starWars))
    store.expectCells (PM.CastTable) (
        ("1", t2, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        ("1", t1, PM.Cast.empty))
    store.expectCells (PM.ActorTable) (
        (id, t2, PO.markHamill))
    store.expectCells (PM.RolesTable) (
        (id, t2, PM.Roles (Seq (PM.Role ("1", "Luke Skywalker")))))
    store.expectCells (PM.Index) (
        ("mark hamill", t2, PO.actors (id)),
        ("star wars", t1, PO.movies ("1")))

    expectReadResult (movies.readActor (t3, id)) (t2, s""" {
        "id": "$id",
        "name": "Mark Hamill",
        "roles":  [ { "movieId": "1", "title": "Star Wars", "role": "Luke Skywalker" } ]
    } """)
  }

  it should "leave the roles unchanged when an actor update is missing it" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 = movies.update (random.nextXid, t0, "1", DO.starWars) .expectPass()

    val (id, t2) = movies.create (random.nextXid, t1, """ {
        "name": "Mark Hamill",
        "roles": [ { "movieId": "1", "role": "Luke Skywalker" } ]
    } """ .fromJson [DM.Actor]) .expectPass()

    val t3 = movies.update (random.nextXid, t2, id, """ {
        "name": "Mark Hamill"
    } """ .fromJson [DM.Actor]) .expectPass()

    store.expectCells (PM.MovieTable) (
        ("1", t1, PO.starWars))
    store.expectCells (PM.CastTable) (
        ("1", t2, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        ("1", t1, PM.Cast.empty))
    store.expectCells (PM.ActorTable) (
        (id, t2, PO.markHamill))
    store.expectCells (PM.RolesTable) (
        (id, t2, PM.Roles (Seq (PM.Role ("1", "Luke Skywalker")))))
    store.expectCells (PM.Index) (
        ("mark hamill", t2, PO.actors (id)),
        ("star wars", t1, PO.movies ("1")))

    expectReadResult (movies.readActor (t3, id)) (t2, s""" {
        "id": "$id",
        "name": "Mark Hamill",
        "roles":  [ { "movieId": "1", "title": "Star Wars", "role": "Luke Skywalker" } ]
    } """)
  }

  it should "leave the role name unchanged when an actor update is missing it" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 = movies.update (random.nextXid, t0, "1", DO.starWars) .expectPass()

    val (id, t2) = movies.create (random.nextXid, t1, """ {
        "name": "Mark Hamill",
        "roles": [ { "movieId": "1", "role": "Luke Skywalker" } ]
    } """ .fromJson [DM.Actor]) .expectPass()

    val t3 = movies.update (random.nextXid, t2, id, """ {
        "name": "Mark Hamill",
        "roles": [ { "movieId": "1" } ]
    } """ .fromJson [DM.Actor]) .expectPass()

    store.expectCells (PM.MovieTable) (
        ("1", t1, PO.starWars))
    store.expectCells (PM.CastTable) (
        ("1", t2, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        ("1", t1, PM.Cast.empty))
    store.expectCells (PM.ActorTable) (
        (id, t2, PO.markHamill))
    store.expectCells (PM.RolesTable) (
        (id, t2, PM.Roles (Seq (PM.Role ("1", "Luke Skywalker")))))
    store.expectCells (PM.Index) (
        ("mark hamill", t2, PO.actors (id)),
        ("star wars", t1, PO.movies ("1")))

    expectReadResult (movies.readActor (t3, id)) (t2, s""" {
        "id": "$id",
        "name": "Mark Hamill",
        "roles":  [ { "movieId": "1", "title": "Star Wars", "role": "Luke Skywalker" } ]
    } """)
  }

  it should "create two actors with the same name" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 = movies.update (random.nextXid, t0, "1", """ {
        "name": "Harrison Ford"
    } """ .fromJson [DM.Actor]) .expectPass()

    val t2 = movies.update (random.nextXid, t1, "2", """ {
        "name": "Harrison Ford"
    } """ .fromJson [DM.Actor]) .expectPass()

    store.expectCells (PM.MovieTable) ()
    store.expectCells (PM.CastTable) ()
    store.expectCells (PM.ActorTable) (
        ("1", t1, PO.harrisonFord),
        ("2", t2, PO.harrisonFord))
    store.expectCells (PM.RolesTable) (
        ("1", t1, PM.Roles.empty),
        ("2", t2, PM.Roles.empty))
    store.expectCells (PM.Index) (
        ("harrison ford", t2, PO.actors ("1", "2")),
        ("harrison ford", t1, PO.actors ("1")))
  }}
