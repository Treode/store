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

package com.treode.disk.stubs.edit

import org.scalatest.Assertions, Assertions.fail

/** Tracks the number of scheduler steps to use for each effect introduced into a scenario. */
private [disk] class Counter [E] {

  /** A scenario. */
  type Effects = Seq [E]

  /** The count of steps used for each effect in the scenario. */
  type Counts = Seq [(E, Int)]

  /** A map from a two phase scenarios to the counts of steps for each effect in each phase. For
    * example, if
    *```
    * (Seq (e1, e2), Seq (e3, e4), true) -> ((1, 2), (3, 4))
    *```
    * is in the map, then we
    * 1. run effect e1 for 1 step in phase 1
    * 2. effect e2 for 2 steps in phase 1
    * 3. crash phase 1
    * 4. run effect 3 for 3 steps in phase 2
    * 5. run effect 4 for 4 steps in phase 2
    */
  private var counts = Map.empty [(Effects, Effects, Boolean), (Counts, Counts)]

  counts += (Seq.empty, Seq.empty, false) -> (Seq.empty, Seq.empty)

  /** Get the counts for a one phase scenario.
    * @param es The effects of the scenario, which we use as a name for the scenario.
    */
  def get (es: Effects): Counts = {
    counts get ((es, Seq.empty, false)) match {
      case Some (cs) => cs._1
      case None => fail (s"Need to test (${es mkString ","})")
    }}

  /** Get the counts for a two phase scenario. We use `(es1, es2, crashed)` as a name for the
    * scenario.
    * @param es1 The effects of the first phase of the scenario.
    * @param es2 The effects of the second phase of the scenario.
    * @param crashed Wether or not phase one crashed in this scenario.
    */
  def get (es1: Effects, es2: Effects, crashed: Boolean): Counts = {
    counts get ((es1, es2, crashed)) match {
      case Some (cs) => cs._2
      case None => fail (s"Need to test (${es1 mkString ","}):crashed (${es2 mkString ","})")
    }}

  def contains (es: Effects): Boolean =
    counts.contains ((es, Seq.empty, false))

  def contains (es1: Effects, es2: Effects, crashed: Boolean): Boolean =
    counts.contains ((es1, es2, crashed))

  /** Add the counts for a one phase scenario.
    * @param es The effects of the scenario, which we use as a name for the scenario.
    * @param cs The counts for each effect of the scenario.
    */
  def add (es: Effects, cs: Counts) {
    if (counts contains (es, Seq.empty, false))
      fail (s"Already tested (${es mkString ","})")
    counts += (es, Seq.empty, false) -> (cs, Seq.empty)
    counts += (es, Seq.empty, true) -> (cs, Seq.empty)
  }

  /** Get the counts for a one phase scenario. We use `(es1, es2, crashed)` as a name for the
    * scenario.
    * @param es1 The effects of the first phase of the scenario.
    * @param es2 The effects of the second phase of the scenario.
    * @param crashed Wether or not phase one crashed in this scenario.
    * @param cs1 The counts for each effect of phase one of the scenario.
    * @param cs2 The counts for each effect of phase two of the scenario.
    */
  def add (es1: Effects, es2: Effects, crashed: Boolean, cs1: Counts, cs2: Counts) {
    if (counts contains (es1, es2, crashed))
      fail (s"Already tested (${es1 mkString ","}):$crashed (${es2 mkString ","})")
    counts += (es1, es2, crashed) -> (cs1, cs2)
  }}
