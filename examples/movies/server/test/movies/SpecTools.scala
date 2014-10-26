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

import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.store.{Bytes, Cell, TxClock, TxId}
import com.treode.store.stubs.StubStore
import com.treode.store.alt.TableDescriptor
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

  def addTitle (ct: TxClock, title: String) (
      implicit random: Random, scheduler: StubScheduler, movies: MovieStore): (String, TxClock) =
    movies
      .create (random.nextXid, t0, DM.Movie ("", title, null, null))
      .expectPass()

  def addName (ct: TxClock, name: String) (
      implicit random: Random, scheduler: StubScheduler, movies: MovieStore): (String, TxClock) =
    movies
      .create (random.nextXid, t0, DM.Actor ("", name, null, null))
      .expectPass()

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

  implicit class RichRandom (r: Random) {

    def nextXid: TxId =
      new TxId (Bytes (r.nextLong), new Instant (0))
  }

  implicit class RichStubStore (store: StubStore) {

    private def thaw [K, V] (d: TableDescriptor [K, V], c: Cell): ExpectedCell [K, V] =
      ExpectedCell (d.key.thaw (c.key), c.time.time, c.value.map (d.value.thaw _))

    def expectCells [K, V] (d: TableDescriptor [K, V]) (expected: ExpectedCell [K, V]*): Unit =
      assertResult (expected) (store.scan (d.id) .map (thaw (d, _)))

    def printCells [K, V] (d: TableDescriptor [K, V]): Unit =
      println (store.scan (d.id) .map (thaw (d, _)) .mkString ("[\n  ", "\n  ", "\n]"))
  }}
