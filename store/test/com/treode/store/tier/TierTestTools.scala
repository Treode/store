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

package com.treode.store.tier

import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.store.{Bytes, Cell, Fruits, Residents, StoreTestTools, TxClock}
import org.scalatest.Assertions

import Assertions.assertResult
import Async.async
import Fruits.{Apple, Tomato}
import TierTable.Meta

private object TierTestTools extends StoreTestTools {

  implicit class TierRichRandom (random: Random) {

    def nextPut (nkeys: Int, nputs: Int): Seq [(Int, Int)] = {

      var keys = Set.empty [Int]
      def nextKey = {
        var key = random.nextInt (nkeys)
        while (keys contains key)
          key = random.nextInt (nkeys)
        keys += key
        key
      }

      def nextValue = random.nextInt (Int.MaxValue)
      Seq.fill (nputs) (nextKey, nextValue)
    }}

  implicit class RichTierTable (table: TierTable) {

    def putCells (cells: Cell*) (implicit scheduler: StubScheduler) {
      for (Cell (key, time, value) <- cells)
        table.put (key, time, value.get)
      scheduler.run()
    }

    def deleteCells (cells: Cell*) (implicit scheduler: StubScheduler) {
      for (Cell (key, time, _) <- cells)
        table.delete (key, time)
      scheduler.run()
    }

    def checkpoint(): Async [Meta] =
      table.checkpoint (Residents.all)

    /** Requires that
      *   - cells are in order
      *   - cell times have gaps
      *   - keys are between Apple and Tomato, exclusive
      */
    def check (cells: Cell*) (implicit scheduler: StubScheduler) {
      for (Seq (c1, c2) <- cells.sliding (2)) {
        require (c1 < c2, "Cells must be in order.")
        require (c1.key != c2.key || c1.time > c2.time+1, s"Times must have gaps.")
        require (Apple < c1.key && c1.key < Tomato, "Key must be between Apple and Tomato.")
      }
      table.iterator (Residents.all) .toSeq.expectPass (cells)
      table.get (Apple, 0) .expectPass (Apple##0)
      table.get (Tomato, 0) .expectPass (Tomato##0)
      for (Seq (c1, c2) <- cells.sliding (2)) {
        table.get (c1.key, c1.time + 1) .expectPass (c1)
        table.get (c1.key, c1.time) .expectPass (c1)
        if (c1.key == c2.key)
          table.get (c1.key, c1.time - 1) .expectPass (c2)
        else
          table.get (c1.key, c1.time - 1) .expectPass (Cell (c1.key, 0, None))
      }}}}
