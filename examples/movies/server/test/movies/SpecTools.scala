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

import scala.language.implicitConversions
import scala.util.Random

import com.fasterxml.jackson.databind.JsonNode
import com.jayway.restassured.response.{Response => RestAssuredResponse}
import com.jayway.restassured.specification.ResponseSpecification
import com.treode.async.stubs.{StubGlobals, StubScheduler}, StubGlobals.scheduler
import com.treode.async.stubs.implicits._
import com.treode.store.{Bytes, Cell, TxClock, TxId}
import com.treode.store.stubs.StubStore
import com.treode.store.alt.TableDescriptor
import com.treode.twitter.finagle.http.filter._
import com.twitter.finagle.Http
import com.twitter.finagle.http.filter.ExceptionFilter
import org.hamcrest.{Description, Matcher, Matchers, TypeSafeMatcher}
import org.joda.time.{DateTime, DateTimeZone, Instant}
import org.scalatest.Assertions

import movies.{DisplayModel => DM, PhysicalModel => PM}
import DateTimeZone.UTC

trait SpecTools {
  this: Assertions =>

  val t0 = TxClock.MinValue

  val sep_25_1951 = new DateTime (1951, 9, 25, 0, 0, 0, UTC)
  val may_25_1977 = new DateTime (1977, 5, 25, 0, 0, 0, UTC)
  val jun_20_1980 = new DateTime (1980, 6, 20, 0, 0, 0, UTC)

  /** Display Objects */
  private [movies] object DO {

    val starWars = DM.Movie ("1", "Star Wars", null, Seq.empty)
    val empireStrikesBack = DM.Movie ("2", "Star Wars: The Empire Strikes Back", null, Seq.empty)
    val returnOfTheJedi = DM.Movie ("3", "Star Wars: Return of the Jedi", null, Seq.empty)

    val markHamill = DM.Actor ("1", "Mark Hamill", null, Seq.empty)
    val harrisonFord = DM.Actor ("2", "Harrison Ford", null, Seq.empty)
    val carrieFisher = DM.Actor ("3", "Carrie Fisher", null, Seq.empty)
  }

  /** Physical Objects */
  private [movies] object PO {

    val starWars = PM.Movie ("Star Wars", null)
    val aNewHope = PM.Movie ("Star Wars: A New Hope", null)
    val empireStrikesBack = PM.Movie ("Star Wars: The Empire Strikes Back", null)
    val returnOfTheJedi = PM.Movie ("Star Wars: Return of the Jedi", null)

    val markHamill = PM.Actor ("Mark Hamill", null)
    val markHammer = PM.Actor ("Mark Hammer", null)
    val harrisonFord = PM.Actor ("Harrison Ford", null)
    val carrieFisher = PM.Actor ("Carrie Fisher", null)

    val thePiano = PM.Movie ("The Piano", null)

    def movies (ids: String*): PM.IndexEntry =
      PM.IndexEntry (ids.toSet, Set.empty)

    def actors (ids: String*): PM.IndexEntry =
      PM.IndexEntry (Set.empty, ids.toSet)
  }

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

  implicit class RichResposne (rsp: RestAssuredResponse) {

    def etag: TxClock = {
      val string = rsp.getHeader ("ETag")
      assert (string != null, "Expected response to have an ETag.")
      val parse = TxClock.parse (string)
      assert (parse.isDefined, s"""Could not parse ETag "$string" as a TxClock""")
      parse.get
    }}

  implicit class RichRandom (r: Random) {

    def nextXid: TxId =
      new TxId (Bytes (Random.nextLong), new Instant (0))
  }

  implicit class RichResponseSpecification (rsp: ResponseSpecification) {

    def etag (ts: TxClock): ResponseSpecification =
      rsp.header ("ETag", ts.toString)
  }

  implicit class RichStubStore (store: StubStore) {

    private def thaw [K, V] (d: TableDescriptor [K, V], c: Cell): ExpectedCell [K, V] =
      ExpectedCell (d.key.thaw (c.key), c.time.time, c.value.map (d.value.thaw _))

    def expectCells [K, V] (d: TableDescriptor [K, V]) (expected: ExpectedCell [K, V]*): Unit =
      assertResult (expected) (store.scan (d.id) .map (thaw (d, _)))

    def printCells [K, V] (d: TableDescriptor [K, V]): Unit =
      println (store.scan (d.id) .map (thaw (d, _)) .mkString ("[\n  ", "\n  ", "\n]"))
  }

  class JsonMatcher (expected: String) extends TypeSafeMatcher [String] {

    def matchesSafely (actual: String): Boolean =
      expected.fromJson [JsonNode] == actual.fromJson [JsonNode]

    def describeTo (desc: Description): Unit =
      desc.appendText (expected);
  }

  def matchesJson (expected: String): Matcher [String] =
    new JsonMatcher (expected)

  def served (test: (Int, StubStore) => MovieStore => Any) {

    val store = StubStore()
    val movies = new MovieStore () (Random, store)
    val router = new Router
    ActorResource (0, movies, router)
    AnalyticsResource (router) (scheduler, store)
    MovieResource (0, movies, router)
    SearchResource (movies, router)

    val port = Random.nextInt (65535 - 49152) + 49152
    val server = Http.serve (
      s":$port",
      NettyToFinagle andThen
      ExceptionFilter andThen
      BadRequestFilter andThen
      JsonExceptionFilter andThen
      router.result)

    try {
      test (port, store) (movies)
    } finally {
      server.close()
    }}

}
