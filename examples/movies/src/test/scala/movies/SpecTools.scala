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

import com.treode.store.{Bytes, Cell, TxClock, TxId}
import com.treode.store.stubs.StubStore
import com.treode.store.alt.TableDescriptor
import org.joda.time.Instant
import org.scalatest.Assertions

trait SpecTools {
  this: Assertions =>

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
