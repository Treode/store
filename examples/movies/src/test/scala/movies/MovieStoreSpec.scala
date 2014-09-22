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

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.JsonMappingException
import com.treode.store.{Bytes, Cell, TxClock, TxId}
import com.treode.store.stubs.StubStore
import com.treode.async.Async
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.store.util.TableDescriptor
import org.joda.time.Instant
import org.scalatest.FlatSpec

import movies.{DisplayModel => DM, PhysicalModel => PM}

class MovieStoreSpec extends FlatSpec {

  val t0 = TxClock.MinValue

  case class ExpectedCell [K, V] (key: K, time: Long, value: Option [V]) {

    override def toString =
      value match {
        case Some (v) => s"($key, $time, ${value.get})"
        case None => s"($key, $time, None)"
      }}

  implicit def optionToExpectedCell [K, V] (v: (K, TxClock, Option [V])): ExpectedCell [K, V] =
    ExpectedCell [K, V] (v._1, v._2.time, v._3)

  implicit def valueToExpectedCell [K, V] (v: (K, TxClock, V)): ExpectedCell [K, V] =
    ExpectedCell [K, V] (v._1, v._2.time, Some (v._3))

  implicit class RichRandom (r: Random) {

    def nextXid: TxId =
      new TxId (Bytes (r.nextLong), new Instant (0))
  }

  implicit class RichStubStore (store: StubStore) {

    private def thaw [K, V] (d: TableDescriptor [K, V], c: Cell): ExpectedCell [K, V] =
      ExpectedCell (d.key.thaw (c.key), c.time.time, c.value.map (d.value.thaw _))

    def expectCells [K, V] (d: TableDescriptor [K, V]) (expected: ExpectedCell [K, V]*): Unit =
      expectResult (expected) (store.scan (d.id) .map (thaw (d, _)))

    def printCells [K, V] (d: TableDescriptor [K, V]): Unit =
      println (store.scan (d.id) .map (thaw (d, _)) .mkString ("[\n  ", "\n  ", "\n]"))
  }

  def setup() = {
    implicit val random = new Random (0)
    implicit val scheduler = StubScheduler.random()
    implicit val store = StubStore()
    val movies = new MovieStore
    (random, scheduler, store, movies)
  }

  def expectReadResult [A] (actual: Async [(TxClock, A)]) (time: TxClock, json: String) (
      implicit s: StubScheduler) {
    val (_t, _v) = actual.expectPass()
    expectResult (time) (_t)
    expectResult (json.fromJson [JsonNode] .toJson) (_v.toJson)
  }

  "The MovieStore" should "create a movie with no cast" in {
    implicit val (random, scheduler, store, movies) = setup()

    val (id, t1) = movies.create (random.nextXid, t0, """ {
        "title": "Star Wars"
    } """ .fromJson [DM.Movie]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (id, t1, PM.Movie ("Star Wars")))
    store.expectCells (PM.MovieTitleIndex) (
        ("Star Wars", t1, id))
    store.expectCells (PM.CastTable) (
        (id, t1, PM.Cast.empty))
    store.expectCells (PM.ActorTable) ()
    store.expectCells (PM.ActorNameIndex) ()
    store.expectCells (PM.RolesTable) ()

    expectReadResult (movies.readMovie (t1, id)) (t1, s""" {
        "id": $id,
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
        (id, t1, PM.Movie ("Star Wars")))
    store.expectCells (PM.MovieTitleIndex) (
        ("Star Wars", t1, id))
    store.expectCells (PM.CastTable) (
        (id, t1, PM.Cast.empty))
    store.expectCells (PM.ActorTable) ()
    store.expectCells (PM.ActorNameIndex) ()
    store.expectCells (PM.RolesTable) ()

    expectReadResult (movies.readMovie (t1, id)) (t1, s""" {
        "id": $id,
        "title": "Star Wars",
        "cast": []
    } """)
  }

  it should "reject a cast member with no actorId" in {
    implicit val (random, scheduler, store, movies) = setup()

    movies.create (random.nextXid, t0, """ {
        "title": "Star Wars",
        "cast": [ { "actor": "Mark Hamill" } ]
    } """ .fromJson [DM.Movie]) .fail [BadRequestException]

    store.expectCells (PM.MovieTable) ()
    store.expectCells (PM.MovieTitleIndex) ()
    store.expectCells (PM.CastTable) ()
    store.expectCells (PM.ActorTable) ()
    store.expectCells (PM.ActorNameIndex) ()
    store.expectCells (PM.RolesTable) ()
  }

  it should "reject a cast member for an actor that does not exist" in {
    implicit val (random, scheduler, store, movies) = setup()

    movies.create (random.nextXid, t0, """ {
        "title": "Star Wars",
        "cast": [ { "actorId": 1 } ]
    } """ .fromJson [DM.Movie]) .fail [BadRequestException]

    store.expectCells (PM.MovieTable) ()
    store.expectCells (PM.MovieTitleIndex) ()
    store.expectCells (PM.CastTable) ()
    store.expectCells (PM.ActorTable) ()
    store.expectCells (PM.ActorNameIndex) ()
    store.expectCells (PM.RolesTable) ()
  }

  it should "create a movie with one cast member" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 =
      movies.update (random.nextXid, t0, 1, DM.Actor (1, "Mark Hamill", Seq.empty)) .expectPass()

    val (id, t2) = movies.create (random.nextXid, t1, """ {
        "title": "Star Wars",
        "cast": [ { "actorId": 1, "role": "Luke Skywalker" } ]
    } """ .fromJson [DM.Movie]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (id, t2, PM.Movie ("Star Wars")))
    store.expectCells (PM.MovieTitleIndex) (
        ("Star Wars", t2, id))
    store.expectCells (PM.CastTable) (
        (id, t2, PM.Cast (Seq (PM.CastMember (1, "Luke Skywalker")))))
    store.expectCells (PM.ActorTable) (
        (1L, t1, PM.Actor ("Mark Hamill")))
    store.expectCells (PM.ActorNameIndex) (
        ("Mark Hamill", t1, 1L))
    store.expectCells (PM.RolesTable) (
        (1L, t2, PM.Roles (Seq (PM.Role (id, "Luke Skywalker")))),
        (1L, t1, PM.Roles.empty))

    expectReadResult (movies.readMovie (t2, id)) (t2, s""" {
        "id": $id,
        "title": "Star Wars",
        "cast": [ { "actorId": 1, "actor": "Mark Hamill", "role": "Luke Skywalker" } ]
    } """)
  }

  it should "create a movie with three cast members" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 =
      movies.update (random.nextXid, t0, 1, DM.Actor (1, "Mark Hamill", Seq.empty)) .expectPass()
    val t2 =
      movies.update (random.nextXid, t0, 2, DM.Actor (2, "Harrison Ford", Seq.empty)) .expectPass()
    val t3 =
      movies.update (random.nextXid, t0, 3, DM.Actor (3, "Carrie Fisher", Seq.empty)) .expectPass()

    val (id, t4) = movies.create (random.nextXid, t3, """ {
        "title": "Star Wars",
        "cast": [
            { "actorId": 1, "role": "Luke Skywalker" },
            { "actorId": 2, "role": "Han Solo" },
            { "actorId": 3, "role": "Princess Leia Organa" }
        ]
    } """ .fromJson [DM.Movie]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (id, t4, PM.Movie ("Star Wars")))
    store.expectCells (PM.MovieTitleIndex) (
        ("Star Wars", t4, id))
    store.expectCells (PM.CastTable) (
        (id, t4, PM.Cast (Seq (
            PM.CastMember (1, "Luke Skywalker"),
            PM.CastMember (2, "Han Solo"),
            PM.CastMember (3, "Princess Leia Organa")))))
    store.expectCells (PM.ActorTable) (
        (1L, t1, PM.Actor ("Mark Hamill")),
        (2L, t2, PM.Actor ("Harrison Ford")),
        (3L, t3, PM.Actor ("Carrie Fisher")))
    store.expectCells (PM.ActorNameIndex) (
        ("Carrie Fisher", t3, 3L),
        ("Harrison Ford", t2, 2L),
        ("Mark Hamill", t1, 1L))
    store.expectCells (PM.RolesTable) (
        (1L, t4, PM.Roles (Seq (PM.Role (id, "Luke Skywalker")))),
        (1L, t1, PM.Roles.empty),
        (2L, t4, PM.Roles (Seq (PM.Role (id, "Han Solo")))),
        (2L, t2, PM.Roles.empty),
        (3L, t4, PM.Roles (Seq (PM.Role (id, "Princess Leia Organa")))),
        (3L, t3, PM.Roles.empty))

    expectReadResult (movies.readMovie (t4, id)) (t4, s""" {
        "id": $id,
        "title": "Star Wars",
        "cast": [
            { "actorId": 1, "actor": "Mark Hamill", "role": "Luke Skywalker" },
            { "actorId": 2, "actor": "Harrison Ford", "role": "Han Solo" },
            { "actorId": 3, "actor": "Carrie Fisher", "role": "Princess Leia Organa" }
        ]
    } """)
  }

  it should "update the movie's primary information" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 =
      movies.update (random.nextXid, t0, 1, DM.Actor (1, "Mark Hamill", Seq.empty)) .expectPass()

    val (id, t2) = movies.create (random.nextXid, t1, """ {
        "title": "Star Wars",
        "cast": [ { "actorId": 1, "role": "Luke Skywalker" } ]
    } """ .fromJson [DM.Movie]) .expectPass()

    val t3 = movies.update (random.nextXid, t2, id, """ {
        "title": "Star Wars: A New Hope",
        "cast": [ { "actorId": 1, "role": "Luke Skywalker" } ]
    } """ .fromJson [DM.Movie]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (id, t3, PM.Movie ("Star Wars: A New Hope")),
        (id, t2, PM.Movie ("Star Wars")))
    store.expectCells (PM.MovieTitleIndex) (
        ("Star Wars", t3, None),
        ("Star Wars", t2, id),
        ("Star Wars: A New Hope", t3, id))
    store.expectCells (PM.CastTable) (
        (id, t2, PM.Cast (Seq (PM.CastMember (1, "Luke Skywalker")))))
    store.expectCells (PM.ActorTable) (
        (1L, t1, PM.Actor ("Mark Hamill")))
    store.expectCells (PM.ActorNameIndex) (
        ("Mark Hamill", t1, 1L))
    store.expectCells (PM.RolesTable) (
        (1L, t2, PM.Roles (Seq (PM.Role (id, "Luke Skywalker")))),
        (1L, t1, PM.Roles.empty))

    expectReadResult (movies.readMovie (t2, id)) (t2, s""" {
        "id": $id,
        "title": "Star Wars",
        "cast": [ { "actorId": 1, "actor": "Mark Hamill", "role": "Luke Skywalker" } ]
    } """)

    expectReadResult (movies.readMovie (t3, id)) (t3, s""" {
        "id": $id,
        "title": "Star Wars: A New Hope",
        "cast": [ { "actorId": 1, "actor": "Mark Hamill", "role": "Luke Skywalker" } ]
    } """)
  }

  it should "update a cast member" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 =
      movies.update (random.nextXid, t0, 1, DM.Actor (1, "Mark Hamill", Seq.empty)) .expectPass()

    val (id, t2) = movies.create (random.nextXid, t1, """ {
        "title": "Star Wars",
        "cast": [ { "actorId": 1, "role": "Luke Skywriter" } ]
    } """ .fromJson [DM.Movie]) .expectPass()

    val t3 = movies.update (random.nextXid, t2, id, """ {
        "title": "Star Wars",
        "cast": [ { "actorId": 1, "role": "Luke Skywalker" } ]
    } """ .fromJson [DM.Movie]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (id, t2, PM.Movie ("Star Wars")))
    store.expectCells (PM.MovieTitleIndex) (
        ("Star Wars", t2, id))
    store.expectCells (PM.CastTable) (
        (id, t3, PM.Cast (Seq (PM.CastMember (1, "Luke Skywalker")))),
        (id, t2, PM.Cast (Seq (PM.CastMember (1, "Luke Skywriter")))))
    store.expectCells (PM.ActorTable) (
        (1L, t1, PM.Actor ("Mark Hamill")))
    store.expectCells (PM.ActorNameIndex) (
        ("Mark Hamill", t1, 1L))
    store.expectCells (PM.RolesTable) (
        (1L, t3, PM.Roles (Seq (PM.Role (id, "Luke Skywalker")))),
        (1L, t2, PM.Roles (Seq (PM.Role (id, "Luke Skywriter")))),
        (1L, t1, PM.Roles.empty))

    expectReadResult (movies.readMovie (t2, id)) (t2, s""" {
        "id": $id,
        "title": "Star Wars",
        "cast": [ { "actorId": 1, "actor": "Mark Hamill", "role": "Luke Skywriter" } ]
    } """)

    expectReadResult (movies.readMovie (t3, id)) (t3, s""" {
        "id": $id,
        "title": "Star Wars",
        "cast": [ { "actorId": 1, "actor": "Mark Hamill", "role": "Luke Skywalker" } ]
    } """)
  }

  it should "add a cast member" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 =
      movies.update (random.nextXid, t0, 1, DM.Actor (1, "Mark Hamill", Seq.empty)) .expectPass()
    val t2 =
      movies.update (random.nextXid, t0, 2, DM.Actor (2, "Harrison Ford", Seq.empty)) .expectPass()
    val t3 =
      movies.update (random.nextXid, t0, 3, DM.Actor (3, "Carrie Fisher", Seq.empty)) .expectPass()

    val (id, t4) = movies.create (random.nextXid, t3, """ {
        "title": "Star Wars",
        "cast": [
            { "actorId": 1, "role": "Luke Skywalker" },
            { "actorId": 3, "role": "Princess Leia Organa" }
        ]
    } """ .fromJson [DM.Movie]) .expectPass()

    val t5 = movies.update (random.nextXid, t4, id, """ {
        "title": "Star Wars",
        "cast": [
            { "actorId": 1, "role": "Luke Skywalker" },
            { "actorId": 2, "role": "Han Solo" },
            { "actorId": 3, "role": "Princess Leia Organa" }
        ]
    } """ .fromJson [DM.Movie]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (id, t4, PM.Movie ("Star Wars")))
    store.expectCells (PM.MovieTitleIndex) (
        ("Star Wars", t4, id))
    store.expectCells (PM.CastTable) (
        (id, t5, PM.Cast (Seq (
            PM.CastMember (1, "Luke Skywalker"),
            PM.CastMember (2, "Han Solo"),
            PM.CastMember (3, "Princess Leia Organa")))),
        (id, t4, PM.Cast (Seq (
            PM.CastMember (1, "Luke Skywalker"),
            PM.CastMember (3, "Princess Leia Organa")))))
    store.expectCells (PM.ActorTable) (
        (1L, t1, PM.Actor ("Mark Hamill")),
        (2L, t2, PM.Actor ("Harrison Ford")),
        (3L, t3, PM.Actor ("Carrie Fisher")))
    store.expectCells (PM.ActorNameIndex) (
        ("Carrie Fisher", t3, 3L),
        ("Harrison Ford", t2, 2L),
        ("Mark Hamill", t1, 1L))
    store.expectCells (PM.RolesTable) (
        (1L, t4, PM.Roles (Seq (PM.Role (id, "Luke Skywalker")))),
        (1L, t1, PM.Roles.empty),
        (2L, t5, PM.Roles (Seq (PM.Role (id, "Han Solo")))),
        (2L, t2, PM.Roles.empty),
        (3L, t4, PM.Roles (Seq (PM.Role (id, "Princess Leia Organa")))),
        (3L, t3, PM.Roles.empty))

    expectReadResult (movies.readMovie (t4, id)) (t4, s""" {
        "id": $id,
        "title": "Star Wars",
        "cast": [
            { "actorId": 1, "actor": "Mark Hamill", "role": "Luke Skywalker" },
            { "actorId": 3, "actor": "Carrie Fisher", "role": "Princess Leia Organa" }
        ]
    } """)

    expectReadResult (movies.readMovie (t5, id)) (t5, s""" {
        "id": $id,
        "title": "Star Wars",
        "cast": [
            { "actorId": 1, "actor": "Mark Hamill", "role": "Luke Skywalker" },
            { "actorId": 2, "actor": "Harrison Ford", "role": "Han Solo" },
            { "actorId": 3, "actor": "Carrie Fisher", "role": "Princess Leia Organa" }
        ]
    } """)
  }

  it should "remove a cast member" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 =
      movies.update (random.nextXid, t0, 1, DM.Actor (1, "Mark Hamill", Seq.empty)) .expectPass()
    val t2 =
      movies.update (random.nextXid, t0, 2, DM.Actor (2, "Harrison Ford", Seq.empty)) .expectPass()
    val t3 =
      movies.update (random.nextXid, t0, 3, DM.Actor (3, "Carrie Fisher", Seq.empty)) .expectPass()

    val (id, t4) = movies.create (random.nextXid, t3, """ {
        "title": "Star Wars",
        "cast": [
            { "actorId": 1, "role": "Luke Skywalker" },
            { "actorId": 2, "role": "Han Solo" },
            { "actorId": 3, "role": "Princess Leia Organa" }
        ]
    } """ .fromJson [DM.Movie]) .expectPass()

    val t5 = movies.update (random.nextXid, t4, id, """ {
        "title": "Star Wars",
        "cast": [
            { "actorId": 1, "role": "Luke Skywalker"},
            { "actorId": 3, "role": "Princess Leia Organa"}
        ]
    } """ .fromJson [DM.Movie]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (id, t4, PM.Movie ("Star Wars")))
    store.expectCells (PM.MovieTitleIndex) (
        ("Star Wars", t4, id))
    store.expectCells (PM.CastTable) (
        (id, t5, PM.Cast (Seq (
            PM.CastMember (1, "Luke Skywalker"),
            PM.CastMember (3, "Princess Leia Organa")))),
        (id, t4, PM.Cast (Seq (
            PM.CastMember (1, "Luke Skywalker"),
            PM.CastMember (2, "Han Solo"),
            PM.CastMember (3, "Princess Leia Organa")))))
    store.expectCells (PM.ActorTable) (
        (1L, t1, PM.Actor ("Mark Hamill")),
        (2L, t2, PM.Actor ("Harrison Ford")),
        (3L, t3, PM.Actor ("Carrie Fisher")))
    store.expectCells (PM.ActorNameIndex) (
        ("Carrie Fisher", t3, 3L),
        ("Harrison Ford", t2, 2L),
        ("Mark Hamill", t1, 1L))
    store.expectCells (PM.RolesTable) (
        (1L, t4, PM.Roles (Seq (PM.Role (id, "Luke Skywalker")))),
        (1L, t1, PM.Roles.empty),
        (2L, t5, PM.Roles.empty),
        (2L, t4, PM.Roles (Seq (PM.Role (id, "Han Solo")))),
        (2L, t2, PM.Roles.empty),
        (3L, t4, PM.Roles (Seq (PM.Role (id, "Princess Leia Organa")))),
        (3L, t3, PM.Roles.empty))

    expectReadResult (movies.readMovie (t4, id)) (t4, s""" {
        "id": $id,
        "title": "Star Wars",
        "cast":  [
            { "actorId": 1, "actor": "Mark Hamill", "role": "Luke Skywalker" },
            { "actorId": 2, "actor": "Harrison Ford", "role": "Han Solo" },
            { "actorId": 3, "actor": "Carrie Fisher", "role": "Princess Leia Organa" }
        ]
    } """)

    expectReadResult (movies.readMovie (t5, id)) (t5, s""" {
        "id": $id,
        "title": "Star Wars",
        "cast":  [
            { "actorId": 1, "actor": "Mark Hamill", "role": "Luke Skywalker" },
            { "actorId": 3, "actor": "Carrie Fisher", "role": "Princess Leia Organa" }
        ]
    } """)
  }

  it should "replace a cast member" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 =
      movies.update (random.nextXid, t0, 1, DM.Actor (1, "Mark Hamill", Seq.empty)) .expectPass()
    val t2 =
      movies.update (random.nextXid, t0, 2, DM.Actor (2, "Harrison Ford", Seq.empty)) .expectPass()
    val t3 =
      movies.update (random.nextXid, t0, 3, DM.Actor (3, "Carrie Fisher", Seq.empty)) .expectPass()

    val (id, t4) = movies.create (random.nextXid, t3, """ {
        "title": "Star Wars",
        "cast": [
            { "actorId": 1, "role": "Luke Skywalker" },
            { "actorId": 2, "role": "Han Solo" }
        ]
    } """ .fromJson [DM.Movie]) .expectPass()

    val t5 = movies.update (random.nextXid, t4, id, """ {
        "title": "Star Wars",
        "cast": [
            { "actorId": 1, "role": "Luke Skywalker" },
            { "actorId": 3, "role": "Princess Leia Organa" }
        ]
    } """ .fromJson [DM.Movie]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (id, t4, PM.Movie ("Star Wars")))
    store.expectCells (PM.MovieTitleIndex) (
        ("Star Wars", t4, id))
    store.expectCells (PM.CastTable) (
        (id, t5, PM.Cast (Seq (
            PM.CastMember (1, "Luke Skywalker"),
            PM.CastMember (3, "Princess Leia Organa")))),
        (id, t4, PM.Cast (Seq (
            PM.CastMember (1, "Luke Skywalker"),
            PM.CastMember (2, "Han Solo")))))
    store.expectCells (PM.ActorTable) (
        (1L, t1, PM.Actor ("Mark Hamill")),
        (2L, t2, PM.Actor ("Harrison Ford")),
        (3L, t3, PM.Actor ("Carrie Fisher")))
    store.expectCells (PM.ActorNameIndex) (
        ("Carrie Fisher", t3, 3L),
        ("Harrison Ford", t2, 2L),
        ("Mark Hamill", t1, 1L))
    store.expectCells (PM.RolesTable) (
        (1L, t4, PM.Roles (Seq (PM.Role (id, "Luke Skywalker")))),
        (1L, t1, PM.Roles.empty),
        (2L, t5, PM.Roles.empty),
        (2L, t4, PM.Roles (Seq (PM.Role (id, "Han Solo")))),
        (2L, t2, PM.Roles.empty),
        (3L, t5, PM.Roles (Seq (PM.Role (id, "Princess Leia Organa")))),
        (3L, t3, PM.Roles.empty))

    expectReadResult (movies.readMovie (t4, id)) (t4, s""" {
        "id": $id,
        "title": "Star Wars",
        "cast":  [
            { "actorId": 1, "actor": "Mark Hamill", "role": "Luke Skywalker" },
            { "actorId": 2, "actor": "Harrison Ford", "role": "Han Solo" }
        ]
    } """)

    expectReadResult (movies.readMovie (t5, id)) (t5, s""" {
        "id": $id,
        "title": "Star Wars",
        "cast":  [
            { "actorId": 1, "actor": "Mark Hamill", "role": "Luke Skywalker" },
            { "actorId": 3, "actor": "Carrie Fisher", "role": "Princess Leia Organa" }
        ]
    } """)
  }

  it should "ignore a movie update with no effect" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 =
      movies.update (random.nextXid, t0, 1, DM.Actor (1, "Mark Hamill", Seq.empty)) .expectPass()

    val (id, t2) = movies.create (random.nextXid, t1, """ {
        "title": "Star Wars",
        "cast": [ { "actorId": 1, "role": "Luke Skywalker" } ]
    } """ .fromJson [DM.Movie]) .expectPass()

    val t3 = movies.update (random.nextXid, t2, id, """ {
        "title": "Star Wars",
        "cast": [ { "actorId": 1, "role": "Luke Skywalker" } ]
    } """ .fromJson [DM.Movie]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (id, t2, PM.Movie ("Star Wars")))
    store.expectCells (PM.MovieTitleIndex) (
        ("Star Wars", t2, id))
    store.expectCells (PM.CastTable) (
        (id, t2, PM.Cast (Seq (PM.CastMember (1, "Luke Skywalker")))))
    store.expectCells (PM.ActorTable) (
        (1L, t1, PM.Actor ("Mark Hamill")))
    store.expectCells (PM.ActorNameIndex) (
        ("Mark Hamill", t1, 1L))
    store.expectCells (PM.RolesTable) (
        (1L, t2, PM.Roles (Seq (PM.Role (id, "Luke Skywalker")))),
        (1L, t1, PM.Roles.empty))

    expectReadResult (movies.readMovie (t2, id)) (t2, s""" {
        "id": $id,
        "title": "Star Wars",
        "cast":  [ { "actorId": 1, "actor": "Mark Hamill", "role": "Luke Skywalker" } ]
    } """)

    expectReadResult (movies.readMovie (t3, id)) (t2, s""" {
        "id": $id,
        "title": "Star Wars",
        "cast":  [ { "actorId": 1, "actor": "Mark Hamill", "role": "Luke Skywalker" } ]
    } """)
  }

  it should "leave the title unchanged when a movie update is missing it" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 =
      movies.update (random.nextXid, t0, 1, DM.Actor (1, "Mark Hamill", Seq.empty)) .expectPass()

    val (id, t2) = movies.create (random.nextXid, t1, """ {
        "title": "Star Wars",
        "cast": [ { "actorId": 1, "role": "Luke Skywalker" } ]
    } """ .fromJson [DM.Movie]) .expectPass()

    val t3 = movies.update (random.nextXid, t2, id, """ {
        "cast": [ { "actorId": 1, "role": "Luke Skywalker" } ]
    } """ .fromJson [DM.Movie]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (id, t2, PM.Movie ("Star Wars")))
    store.expectCells (PM.MovieTitleIndex) (
        ("Star Wars", t2, id))
    store.expectCells (PM.CastTable) (
        (id, t2, PM.Cast (Seq (PM.CastMember (1, "Luke Skywalker")))))
    store.expectCells (PM.ActorTable) (
        (1L, t1, PM.Actor ("Mark Hamill")))
    store.expectCells (PM.ActorNameIndex) (
        ("Mark Hamill", t1, 1L))
    store.expectCells (PM.RolesTable) (
        (1L, t2, PM.Roles (Seq (PM.Role (id, "Luke Skywalker")))),
        (1L, t1, PM.Roles.empty))

    expectReadResult (movies.readMovie (t3, id)) (t2, s""" {
        "id": $id,
        "title": "Star Wars",
        "cast": [ { "actorId": 1, "actor": "Mark Hamill", "role": "Luke Skywalker" } ]
    } """)
  }

  it should "leave the cast unchanged when a movie update is missing it" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 =
      movies.update (random.nextXid, t0, 1, DM.Actor (1, "Mark Hamill", Seq.empty)) .expectPass()

    val (id, t2) = movies.create (random.nextXid, t1, """ {
        "title": "Star Wars",
        "cast": [ { "actorId": 1, "role": "Luke Skywalker" } ]
    } """ .fromJson [DM.Movie]) .expectPass()

    val t3 = movies.update (random.nextXid, t2, id, """ {
        "title": "Star Wars"
    } """ .fromJson [DM.Movie]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (id, t2, PM.Movie ("Star Wars")))
    store.expectCells (PM.MovieTitleIndex) (
        ("Star Wars", t2, id))
    store.expectCells (PM.CastTable) (
        (id, t2, PM.Cast (Seq (PM.CastMember (1, "Luke Skywalker")))))
    store.expectCells (PM.ActorTable) (
        (1L, t1, PM.Actor ("Mark Hamill")))
    store.expectCells (PM.ActorNameIndex) (
        ("Mark Hamill", t1, 1L))
    store.expectCells (PM.RolesTable) (
        (1L, t2, PM.Roles (Seq (PM.Role (id, "Luke Skywalker")))),
        (1L, t1, PM.Roles.empty))

    expectReadResult (movies.readMovie (t3, id)) (t2, s""" {
        "id": $id,
        "title": "Star Wars",
        "cast": [ { "actorId": 1, "actor": "Mark Hamill", "role": "Luke Skywalker" } ]
    } """)
  }

  it should "leave the role name unchanged when a movie update is missing it" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 =
      movies.update (random.nextXid, t0, 1, DM.Actor (1, "Mark Hamill", Seq.empty)) .expectPass()

    val (id, t2) = movies.create (random.nextXid, t1, """ {
        "title": "Star Wars",
        "cast": [ { "actorId": 1, "role": "Luke Skywalker" } ]
    } """ .fromJson [DM.Movie]) .expectPass()

    val t3 = movies.update (random.nextXid, t2, id, """ {
        "title": "Star Wars",
        "cast": [ { "actorId": 1 } ]
    } """ .fromJson [DM.Movie]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (id, t2, PM.Movie ("Star Wars")))
    store.expectCells (PM.MovieTitleIndex) (
        ("Star Wars", t2, id))
    store.expectCells (PM.CastTable) (
        (id, t2, PM.Cast (Seq (PM.CastMember (1, "Luke Skywalker")))))
    store.expectCells (PM.ActorTable) (
        (1L, t1, PM.Actor ("Mark Hamill")))
    store.expectCells (PM.ActorNameIndex) (
        ("Mark Hamill", t1, 1L))
    store.expectCells (PM.RolesTable) (
        (1L, t2, PM.Roles (Seq (PM.Role (id, "Luke Skywalker")))),
        (1L, t1, PM.Roles.empty))

    expectReadResult (movies.readMovie (t3, id)) (t2, s""" {
        "id": $id,
        "title": "Star Wars",
        "cast": [ { "actorId": 1, "actor": "Mark Hamill", "role": "Luke Skywalker" } ]
    } """)
  }

  it should "create an actor with no roles" in {
    implicit val (random, scheduler, store, movies) = setup()

    val (id, t1) = movies.create (random.nextXid, t0, """ {
        "name": "Mark Hamill"
    } """ .fromJson [DM.Actor]) .expectPass()

    store.expectCells (PM.MovieTable) ()
    store.expectCells (PM.MovieTitleIndex) ()
    store.expectCells (PM.CastTable) ()
    store.expectCells (PM.ActorTable) (
        (id, t1, PM.Actor ("Mark Hamill")))
    store.expectCells (PM.ActorNameIndex) (
        ("Mark Hamill", t1, id))
    store.expectCells (PM.RolesTable) (
        (id, t1, PM.Roles.empty))

    expectReadResult (movies.readActor (t1, id)) (t1, s""" {
        "id": $id,
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
    store.expectCells (PM.MovieTitleIndex) ()
    store.expectCells (PM.CastTable) ()
    store.expectCells (PM.ActorTable) (
        (id, t1, PM.Actor ("Mark Hamill")))
    store.expectCells (PM.ActorNameIndex) (
        ("Mark Hamill", t1, id))
    store.expectCells (PM.RolesTable) (
        (id, t1, PM.Roles.empty))

    expectReadResult (movies.readActor (t1, id)) (t1, s""" {
        "id": $id,
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
    store.expectCells (PM.MovieTitleIndex) ()
    store.expectCells (PM.CastTable) ()
    store.expectCells (PM.ActorTable) ()
    store.expectCells (PM.ActorNameIndex) ()
    store.expectCells (PM.RolesTable) ()
  }

  it should "reject a role for a movie that does not exist" in {
    implicit val (random, scheduler, store, movies) = setup()

    movies.create (random.nextXid, t0, """ {
        "name": "Mark Hamill",
        "roles": [ { "movieId": 1 } ]
    } """ .fromJson [DM.Actor]) .fail [BadRequestException]

    store.expectCells (PM.MovieTable) ()
    store.expectCells (PM.MovieTitleIndex) ()
    store.expectCells (PM.CastTable) ()
    store.expectCells (PM.ActorTable) ()
    store.expectCells (PM.ActorNameIndex) ()
    store.expectCells (PM.RolesTable) ()
  }

  it should "create an actor with one role" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 =
      movies.update (random.nextXid, t0, 1, DM.Movie (1, "Star Wars", Seq.empty)) .expectPass()

    val (id, t2) = movies.create (random.nextXid, t1, """ {
        "name": "Mark Hamill",
        "roles": [ { "movieId": 1, "role": "Luke Skywalker" } ]
    } """ .fromJson [DM.Actor]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (1L, t1, PM.Movie ("Star Wars")))
    store.expectCells (PM.MovieTitleIndex) (
        ("Star Wars", t1, 1L))
    store.expectCells (PM.CastTable) (
        (1L, t2, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        (1L, t1, PM.Cast.empty))
    store.expectCells (PM.ActorTable) (
        (id, t2, PM.Actor ("Mark Hamill")))
    store.expectCells (PM.ActorNameIndex) (
        ("Mark Hamill", t2, id))
    store.expectCells (PM.RolesTable) (
        (id, t2, PM.Roles (Seq (PM.Role (1, "Luke Skywalker")))))

    expectReadResult (movies.readActor (t2, id)) (t2, s""" {
        "id": $id,
        "name": "Mark Hamill",
        "roles":  [ { "movieId": 1, "title": "Star Wars", "role": "Luke Skywalker" } ]
    } """)
  }

  it should "create an actor with three roles" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 =
      movies.update (random.nextXid, t0, 1, DM.Movie (1, "Star Wars", Seq.empty)) .expectPass()
    val t2 =
      movies.update (random.nextXid, t0, 2, DM.Movie (2, "Star Wars: The Empire Strikes Back", Seq.empty)) .expectPass()
    val t3 =
      movies.update (random.nextXid, t0, 3, DM.Movie (3, "Star Wars: Return of the Jedi", Seq.empty)) .expectPass()

    val (id, t4) = movies.create (random.nextXid, t3, """ {
        "name": "Mark Hamill",
        "roles": [
            { "movieId": 1, "role": "Luke Skywalker" },
            { "movieId": 2, "role": "Luke Skywalker" },
            { "movieId": 3, "role": "Luke Skywalker" }
        ]
    } """ .fromJson [DM.Actor]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (1L, t1, PM.Movie ("Star Wars")),
        (2L, t2, PM.Movie ("Star Wars: The Empire Strikes Back")),
        (3L, t3, PM.Movie ("Star Wars: Return of the Jedi")))
    store.expectCells (PM.MovieTitleIndex) (
        ("Star Wars", t1, 1L),
        ("Star Wars: Return of the Jedi", t3, 3L),
        ("Star Wars: The Empire Strikes Back", t2, 2L))
    store.expectCells (PM.CastTable) (
        (1L, t4, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        (1L, t1, PM.Cast.empty),
        (2L, t4, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        (2L, t2, PM.Cast.empty),
        (3L, t4, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        (3L, t3, PM.Cast.empty))
    store.expectCells (PM.ActorTable) (
        (id, t4, PM.Actor ("Mark Hamill")))
    store.expectCells (PM.ActorNameIndex) (
        ("Mark Hamill", t4, id))
    store.expectCells (PM.RolesTable) (
        (id, t4, PM.Roles (Seq (
            PM.Role (1, "Luke Skywalker"),
            PM.Role (2, "Luke Skywalker"),
            PM.Role (3, "Luke Skywalker")))))

    expectReadResult (movies.readActor (t4, id)) (t4, s""" {
        "id": $id,
        "name": "Mark Hamill",
        "roles":  [
            { "movieId": 1, "title": "Star Wars", "role": "Luke Skywalker" },
            { "movieId": 2, "title": "Star Wars: The Empire Strikes Back", "role": "Luke Skywalker" },
            { "movieId": 3, "title": "Star Wars: Return of the Jedi", "role": "Luke Skywalker" }
        ]
    } """)
  }

  it should "update the actor's primary information" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 =
      movies.update (random.nextXid, t0, 1, DM.Movie (1, "Star Wars", Seq.empty)) .expectPass()

    val (id, t2) = movies.create (random.nextXid, t1, """ {
        "name": "Mark Hammer",
        "roles": [ { "movieId": 1, "role": "Luke Skywalker" } ]
    } """ .fromJson [DM.Actor]) .expectPass()

    val t3 = movies.update (random.nextXid, t2, id, """ {
        "name": "Mark Hamill",
        "roles": [ { "movieId": 1, "role": "Luke Skywalker" } ]
    } """ .fromJson [DM.Actor]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (1L, t1, PM.Movie ("Star Wars")))
    store.expectCells (PM.MovieTitleIndex) (
        ("Star Wars", t1, 1L))
    store.expectCells (PM.CastTable) (
        (1L, t2, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        (1L, t1, PM.Cast.empty))
    store.expectCells (PM.ActorTable) (
        (id, t3, PM.Actor ("Mark Hamill")),
        (id, t2, PM.Actor ("Mark Hammer")))
    store.expectCells (PM.ActorNameIndex) (
        ("Mark Hamill", t3, id),
        ("Mark Hammer", t3, None),
        ("Mark Hammer", t2, id))
    store.expectCells (PM.RolesTable) (
        (id, t2, PM.Roles (Seq (PM.Role (1, "Luke Skywalker")))))

    expectReadResult (movies.readActor (t2, id)) (t2, s""" {
        "id": $id,
        "name": "Mark Hammer",
        "roles":  [ { "movieId": 1, "title": "Star Wars", "role": "Luke Skywalker" } ]
    } """)

    expectReadResult (movies.readActor (t3, id)) (t3, s""" {
        "id": $id,
        "name": "Mark Hamill",
        "roles":  [ { "movieId": 1, "title": "Star Wars", "role": "Luke Skywalker" } ]
    } """)
  }

  it should "update a role" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 =
      movies.update (random.nextXid, t0, 1, DM.Movie (1, "Star Wars", Seq.empty)) .expectPass()

    val (id, t2) = movies.create (random.nextXid, t1, """ {
        "name": "Mark Hamill",
        "roles": [ { "movieId": 1, "role": "Luke Skywriter" } ]
    } """ .fromJson [DM.Actor]) .expectPass()

    val t3 = movies.update (random.nextXid, t2, id, """ {
        "name": "Mark Hamill",
        "roles": [ { "movieId": 1, "role": "Luke Skywalker" } ]
    } """ .fromJson [DM.Actor]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (1L, t1, PM.Movie ("Star Wars")))
    store.expectCells (PM.MovieTitleIndex) (
        ("Star Wars", t1, 1L))
    store.expectCells (PM.CastTable) (
        (1L, t3, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        (1L, t2, PM.Cast (Seq (PM.CastMember (id, "Luke Skywriter")))),
        (1L, t1, PM.Cast.empty))
    store.expectCells (PM.ActorTable) (
        (id, t2, PM.Actor ("Mark Hamill")))
    store.expectCells (PM.ActorNameIndex) (
        ("Mark Hamill", t2, id))
    store.expectCells (PM.RolesTable) (
        (id, t3, PM.Roles (Seq (PM.Role (1, "Luke Skywalker")))),
        (id, t2, PM.Roles (Seq (PM.Role (1, "Luke Skywriter")))))

    expectReadResult (movies.readActor (t2, id)) (t2, s""" {
        "id": $id,
        "name": "Mark Hamill",
        "roles":  [ { "movieId": 1, "title": "Star Wars", "role": "Luke Skywriter" } ]
    } """)

    expectReadResult (movies.readActor (t3, id)) (t3, s""" {
        "id": $id,
        "name": "Mark Hamill",
        "roles":  [ { "movieId": 1, "title": "Star Wars", "role": "Luke Skywalker" } ]
    } """)
  }

  it should "add a role" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 =
      movies.update (random.nextXid, t0, 1, DM.Movie (1, "Star Wars", Seq.empty)) .expectPass()
    val t2 =
      movies.update (random.nextXid, t0, 2, DM.Movie (2, "Star Wars: The Empire Strikes Back", Seq.empty)) .expectPass()
    val t3 =
      movies.update (random.nextXid, t0, 3, DM.Movie (3, "Star Wars: Return of the Jedi", Seq.empty)) .expectPass()

    val (id, t4) = movies.create (random.nextXid, t3, """ {
        "name": "Mark Hamill",
        "roles": [
            { "movieId": 1, "role": "Luke Skywalker" },
            { "movieId": 3, "role": "Luke Skywalker" }
        ]
    } """ .fromJson [DM.Actor]) .expectPass()

    val t5 = movies.update (random.nextXid, t4, id, """ {
        "name": "Mark Hamill",
        "roles": [
            { "movieId": 1, "role": "Luke Skywalker" },
            { "movieId": 2, "role": "Luke Skywalker" },
            { "movieId": 3, "role": "Luke Skywalker" }
        ]
    } """ .fromJson [DM.Actor]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (1L, t1, PM.Movie ("Star Wars")),
        (2L, t2, PM.Movie ("Star Wars: The Empire Strikes Back")),
        (3L, t3, PM.Movie ("Star Wars: Return of the Jedi")))
    store.expectCells (PM.MovieTitleIndex) (
        ("Star Wars", t1, 1L),
        ("Star Wars: Return of the Jedi", t3, 3L),
        ("Star Wars: The Empire Strikes Back", t2, 2L))
    store.expectCells (PM.CastTable) (
        (1L, t4, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        (1L, t1, PM.Cast.empty),
        (2L, t5, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        (2L, t2, PM.Cast.empty),
        (3L, t4, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        (3L, t3, PM.Cast.empty))
    store.expectCells (PM.ActorTable) (
        (id, t4, PM.Actor ("Mark Hamill")))
    store.expectCells (PM.ActorNameIndex) (
        ("Mark Hamill", t4, id))
    store.expectCells (PM.RolesTable) (
        (id, t5, PM.Roles (Seq (
            PM.Role (1, "Luke Skywalker"),
            PM.Role (2, "Luke Skywalker"),
            PM.Role (3, "Luke Skywalker")))),
        (id, t4, PM.Roles (Seq (
            PM.Role (1, "Luke Skywalker"),
            PM.Role (3, "Luke Skywalker")))))

    expectReadResult (movies.readActor (t4, id)) (t4, s""" {
        "id": $id,
        "name": "Mark Hamill",
        "roles":  [
            { "movieId": 1, "title": "Star Wars", "role": "Luke Skywalker" },
            { "movieId": 3, "title": "Star Wars: Return of the Jedi", "role": "Luke Skywalker" }
        ]
    } """)

    expectReadResult (movies.readActor (t5, id)) (t5, s""" {
        "id": $id,
        "name": "Mark Hamill",
        "roles":  [
            { "movieId": 1, "title": "Star Wars", "role": "Luke Skywalker" },
            { "movieId": 2, "title": "Star Wars: The Empire Strikes Back", "role": "Luke Skywalker" },
            { "movieId": 3, "title": "Star Wars: Return of the Jedi", "role": "Luke Skywalker" }
        ]
    } """)
  }

  it should "remove a role" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 =
      movies.update (random.nextXid, t0, 1, DM.Movie (1, "Star Wars", Seq.empty)) .expectPass()
    val t2 =
      movies.update (random.nextXid, t0, 2, DM.Movie (2, "Star Wars: The Empire Strikes Back", Seq.empty)) .expectPass()
    val t3 =
      movies.update (random.nextXid, t0, 3, DM.Movie (3, "Star Wars: Return of the Jedi", Seq.empty)) .expectPass()

    val (id, t4) = movies.create (random.nextXid, t3, """ {
        "name": "Mark Hamill",
        "roles": [
            { "movieId": 1, "role": "Luke Skywalker" },
            { "movieId": 2, "role": "Luke Skywalker" },
            { "movieId": 3, "role": "Luke Skywalker" }
        ]
    } """ .fromJson [DM.Actor]) .expectPass()

    val t5 = movies.update (random.nextXid, t4, id, """ {
        "name": "Mark Hamill",
        "roles": [
            { "movieId": 1, "role": "Luke Skywalker" },
            { "movieId": 3, "role": "Luke Skywalker" }
        ]
    } """ .fromJson [DM.Actor]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (1L, t1, PM.Movie ("Star Wars")),
        (2L, t2, PM.Movie ("Star Wars: The Empire Strikes Back")),
        (3L, t3, PM.Movie ("Star Wars: Return of the Jedi")))
    store.expectCells (PM.MovieTitleIndex) (
        ("Star Wars", t1, 1L),
        ("Star Wars: Return of the Jedi", t3, 3L),
        ("Star Wars: The Empire Strikes Back", t2, 2L))
    store.expectCells (PM.CastTable) (
        (1L, t4, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        (1L, t1, PM.Cast.empty),
        (2L, t5, PM.Cast.empty),
        (2L, t4, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        (2L, t2, PM.Cast.empty),
        (3L, t4, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        (3L, t3, PM.Cast.empty))
    store.expectCells (PM.ActorTable) (
        (id, t4, PM.Actor ("Mark Hamill")))
    store.expectCells (PM.ActorNameIndex) (
        ("Mark Hamill", t4, id))
    store.expectCells (PM.RolesTable) (
        (id, t5, PM.Roles (Seq (
            PM.Role (1, "Luke Skywalker"),
            PM.Role (3, "Luke Skywalker")))),
        (id, t4, PM.Roles (Seq (
            PM.Role (1, "Luke Skywalker"),
            PM.Role (2, "Luke Skywalker"),
            PM.Role (3, "Luke Skywalker")))))

    expectReadResult (movies.readActor (t4, id)) (t4, s""" {
        "id": $id,
        "name": "Mark Hamill",
        "roles":  [
            { "movieId": 1, "title": "Star Wars", "role": "Luke Skywalker" },
            { "movieId": 2, "title": "Star Wars: The Empire Strikes Back", "role": "Luke Skywalker" },
            { "movieId": 3, "title": "Star Wars: Return of the Jedi", "role": "Luke Skywalker" }
        ]
    } """)

    expectReadResult (movies.readActor (t5, id)) (t5, s""" {
        "id": $id,
        "name": "Mark Hamill",
        "roles":  [
            { "movieId": 1, "title": "Star Wars", "role": "Luke Skywalker" },
            { "movieId": 3, "title": "Star Wars: Return of the Jedi", "role": "Luke Skywalker" }
        ]
    } """)
  }

  it should "replace a role" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 =
      movies.update (random.nextXid, t0, 1, DM.Movie (1, "Star Wars", Seq.empty)) .expectPass()
    val t2 =
      movies.update (random.nextXid, t0, 2, DM.Movie (2, "Star Wars: The Empire Strikes Back", Seq.empty)) .expectPass()
    val t3 =
      movies.update (random.nextXid, t0, 3, DM.Movie (3, "Star Wars: Return of the Jedi", Seq.empty)) .expectPass()

    val (id, t4) = movies.create (random.nextXid, t3, """ {
        "name": "Mark Hamill",
        "roles": [
            { "movieId": 1, "role": "Luke Skywalker" },
            { "movieId": 2, "role": "Luke Skywalker" }
        ]
    } """ .fromJson [DM.Actor]) .expectPass()

    val t5 = movies.update (random.nextXid, t4, id, """ {
        "name": "Mark Hamill",
        "roles": [
            { "movieId": 1, "role": "Luke Skywalker" },
            { "movieId": 3, "role": "Luke Skywalker" }
        ]
    } """ .fromJson [DM.Actor]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (1L, t1, PM.Movie ("Star Wars")),
        (2L, t2, PM.Movie ("Star Wars: The Empire Strikes Back")),
        (3L, t3, PM.Movie ("Star Wars: Return of the Jedi")))
    store.expectCells (PM.MovieTitleIndex) (
        ("Star Wars", t1, 1L),
        ("Star Wars: Return of the Jedi", t3, 3L),
        ("Star Wars: The Empire Strikes Back", t2, 2L))
    store.expectCells (PM.CastTable) (
        (1L, t4, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        (1L, t1, PM.Cast.empty),
        (2L, t5, PM.Cast.empty),
        (2L, t4, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        (2L, t2, PM.Cast.empty),
        (3L, t5, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        (3L, t3, PM.Cast.empty))
    store.expectCells (PM.ActorTable) (
        (id, t4, PM.Actor ("Mark Hamill")))
    store.expectCells (PM.ActorNameIndex) (
        ("Mark Hamill", t4, id))
    store.expectCells (PM.RolesTable) (
        (id, t5, PM.Roles (Seq (
            PM.Role (1, "Luke Skywalker"),
            PM.Role (3, "Luke Skywalker")))),
        (id, t4, PM.Roles (Seq (
            PM.Role (1, "Luke Skywalker"),
            PM.Role (2, "Luke Skywalker")))))

    expectReadResult (movies.readActor (t4, id)) (t4, s""" {
        "id": $id,
        "name": "Mark Hamill",
        "roles":  [
            { "movieId": 1, "title": "Star Wars", "role": "Luke Skywalker" },
            { "movieId": 2, "title": "Star Wars: The Empire Strikes Back", "role": "Luke Skywalker" }
        ]
    } """)

    expectReadResult (movies.readActor (t5, id)) (t5, s""" {
        "id": $id,
        "name": "Mark Hamill",
        "roles":  [
            { "movieId": 1, "title": "Star Wars", "role": "Luke Skywalker" },
            { "movieId": 3, "title": "Star Wars: Return of the Jedi", "role": "Luke Skywalker" }
        ]
    } """)
  }

  it should "ignore an actor update with no effect" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 =
      movies.update (random.nextXid, t0, 1, DM.Movie (1, "Star Wars", Seq.empty)) .expectPass()

    val (id, t2) = movies.create (random.nextXid, t1, """ {
        "name": "Mark Hamill",
        "roles": [ { "movieId": 1, "role": "Luke Skywalker" } ]
    } """ .fromJson [DM.Actor]) .expectPass()

    val t3 = movies.update (random.nextXid, t2, id, """ {
        "name": "Mark Hamill",
        "roles": [ { "movieId": 1, "role": "Luke Skywalker" } ]
    } """ .fromJson [DM.Actor]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (1L, t1, PM.Movie ("Star Wars")))
    store.expectCells (PM.MovieTitleIndex) (
        ("Star Wars", t1, 1L))
    store.expectCells (PM.CastTable) (
        (1L, t2, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        (1L, t1, PM.Cast.empty))
    store.expectCells (PM.ActorTable) (
        (id, t2, PM.Actor ("Mark Hamill")))
    store.expectCells (PM.ActorNameIndex) (
        ("Mark Hamill", t2, id))
    store.expectCells (PM.RolesTable) (
        (id, t2, PM.Roles (Seq (PM.Role (1, "Luke Skywalker")))))

    expectReadResult (movies.readActor (t2, id)) (t2, s""" {
        "id": $id,
        "name": "Mark Hamill",
        "roles":  [ { "movieId": 1, "title": "Star Wars", "role": "Luke Skywalker" } ]
    } """)

    expectReadResult (movies.readActor (t3, id)) (t2, s""" {
        "id": $id,
        "name": "Mark Hamill",
        "roles":  [ { "movieId": 1, "title": "Star Wars", "role": "Luke Skywalker" } ]
    } """)
  }

  it should "leave the name unchanged when an actor update is missing it" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 =
      movies.update (random.nextXid, t0, 1, DM.Movie (1, "Star Wars", Seq.empty)) .expectPass()

    val (id, t2) = movies.create (random.nextXid, t1, """ {
        "name": "Mark Hamill",
        "roles": [ { "movieId": 1, "role": "Luke Skywalker" } ]
    } """ .fromJson [DM.Actor]) .expectPass()

    val t3 = movies.update (random.nextXid, t2, id, """ {
        "roles": [ { "movieId": 1, "role": "Luke Skywalker" } ]
    } """ .fromJson [DM.Actor]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (1L, t1, PM.Movie ("Star Wars")))
    store.expectCells (PM.MovieTitleIndex) (
        ("Star Wars", t1, 1L))
    store.expectCells (PM.CastTable) (
        (1L, t2, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        (1L, t1, PM.Cast.empty))
    store.expectCells (PM.ActorTable) (
        (id, t2, PM.Actor ("Mark Hamill")))
    store.expectCells (PM.ActorNameIndex) (
        ("Mark Hamill", t2, id))
    store.expectCells (PM.RolesTable) (
        (id, t2, PM.Roles (Seq (PM.Role (1, "Luke Skywalker")))))

    expectReadResult (movies.readActor (t3, id)) (t2, s""" {
        "id": $id,
        "name": "Mark Hamill",
        "roles":  [ { "movieId": 1, "title": "Star Wars", "role": "Luke Skywalker" } ]
    } """)
  }

  it should "leave the roles unchanged when an actor update is missing it" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 =
      movies.update (random.nextXid, t0, 1, DM.Movie (1, "Star Wars", Seq.empty)) .expectPass()

    val (id, t2) = movies.create (random.nextXid, t1, """ {
        "name": "Mark Hamill",
        "roles": [ { "movieId": 1, "role": "Luke Skywalker" } ]
    } """ .fromJson [DM.Actor]) .expectPass()

    val t3 = movies.update (random.nextXid, t2, id, """ {
        "name": "Mark Hamill"
    } """ .fromJson [DM.Actor]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (1L, t1, PM.Movie ("Star Wars")))
    store.expectCells (PM.MovieTitleIndex) (
        ("Star Wars", t1, 1L))
    store.expectCells (PM.CastTable) (
        (1L, t2, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        (1L, t1, PM.Cast.empty))
    store.expectCells (PM.ActorTable) (
        (id, t2, PM.Actor ("Mark Hamill")))
    store.expectCells (PM.ActorNameIndex) (
        ("Mark Hamill", t2, id))
    store.expectCells (PM.RolesTable) (
        (id, t2, PM.Roles (Seq (PM.Role (1, "Luke Skywalker")))))

    expectReadResult (movies.readActor (t3, id)) (t2, s""" {
        "id": $id,
        "name": "Mark Hamill",
        "roles":  [ { "movieId": 1, "title": "Star Wars", "role": "Luke Skywalker" } ]
    } """)
  }

  it should "leave the role name unchanged when an actor update is missing it" in {
    implicit val (random, scheduler, store, movies) = setup()

    val t1 =
      movies.update (random.nextXid, t0, 1, DM.Movie (1, "Star Wars", Seq.empty)) .expectPass()

    val (id, t2) = movies.create (random.nextXid, t1, """ {
        "name": "Mark Hamill",
        "roles": [ { "movieId": 1, "role": "Luke Skywalker" } ]
    } """ .fromJson [DM.Actor]) .expectPass()

    val t3 = movies.update (random.nextXid, t2, id, """ {
        "name": "Mark Hamill",
        "roles": [ { "movieId": 1 } ]
    } """ .fromJson [DM.Actor]) .expectPass()

    store.expectCells (PM.MovieTable) (
        (1L, t1, PM.Movie ("Star Wars")))
    store.expectCells (PM.MovieTitleIndex) (
        ("Star Wars", t1, 1L))
    store.expectCells (PM.CastTable) (
        (1L, t2, PM.Cast (Seq (PM.CastMember (id, "Luke Skywalker")))),
        (1L, t1, PM.Cast.empty))
    store.expectCells (PM.ActorTable) (
        (id, t2, PM.Actor ("Mark Hamill")))
    store.expectCells (PM.ActorNameIndex) (
        ("Mark Hamill", t2, id))
    store.expectCells (PM.RolesTable) (
        (id, t2, PM.Roles (Seq (PM.Role (1, "Luke Skywalker")))))

    expectReadResult (movies.readActor (t3, id)) (t2, s""" {
        "id": $id,
        "name": "Mark Hamill",
        "roles":  [ { "movieId": 1, "title": "Star Wars", "role": "Luke Skywalker" } ]
    } """)
  }}
