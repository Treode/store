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

package com.treode.disk.stubs

import org.scalatest.Assertions, Assertions.fail

/** Tracks the number of scheduler steps to use for each effect introduced into a scenario. */
private [disk] class Counter [E] {

  /** A scenario. */
  type Effects = Seq [E]

  case class Phase (effects: Effects, crashed: Boolean) {

    assert (!effects.isEmpty)

    override def toString: String =
      s"(${effects mkString ", "}${if (crashed) ", CRASH" else ""})"
  }

  type Phases = Seq [Phase]

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
  private var counts = Map.empty [Phases, Counts]
  counts += Seq.empty -> Seq.empty

  private def _get (ps: Phases): Counts =
    counts get (ps) match {
      case Some (cs) => cs
      case None => fail (s"Need to test ${ps mkString ", "}")
    }

  private def _add (ps: Phases, cs: Counts) {
    if (counts contains ps)
      fail (s"Already tested (${ps mkString ", "})")
    counts += ps -> cs
  }

  /** The name of a one phase scenario. */
  private def _phases (es: Effects): Seq [Phase] =
    Seq (Phase (es, false))

  /** The name of a two phase scenario. */
  private def _phases (es1: Effects, c1: Boolean, es2: Effects): Seq [Phase] =
    Seq (Phase (es1, c1), Phase (es2, false))

  /** The name of a three phase scenario. */
  private def _phases (es1: Effects, c1: Boolean, es2: Effects, c2: Boolean, es3: Effects): Seq [Phase] =
    Seq (Phase (es1, c1), Phase (es2, c2), Phase (es3, false))

  /** Get the counts for a one phase scenario.
    * @param es The effects of the scenario, which we use as a name for the scenario.
    */
  def get (es: Effects): Counts =
    if (es.isEmpty)
      Seq.empty
    else
      _get (_phases (es))

  /** Add the counts for a one phase scenario.
    * @param es The effects of the scenario, which we use as a name for the scenario.
    * @param cs The counts for each effect of the scenario.
    */
  def add (es: Effects, cs: Counts): Unit =
    _add (_phases (es), cs)

  /** Get the counts for a two phase scenario.
    * @param es1 The effects of the first phase of the scenario.
    * @param c1 Wether or not phase one crashed in this scenario.
    * @param es2 The effects of the second phase of the scenario.
    */
  def get (es1: Effects, c1: Boolean, es2: Effects): Counts =
    if (es2.isEmpty)
      Seq.empty
    else
      _get (_phases (es1, c1, es2))

  /** Add the counts for a two phase scenario.
    * @param es1 The effects of the first phase of the scenario.
    * @param c1 Wether or not phase one crashed in this scenario.
    * @param es2 The effects of the second phase of the scenario.
    * @param cs2 The counts for each effect of phase two of the scenario.
    */
  def add (es1: Effects, c1: Boolean, es2: Effects, cs2: Counts): Unit =
    _add (_phases (es1, c1, es2), cs2)

  /** Get the counts for a three phase scenario.
    * @param es1 The effects of the first phase of the scenario.
    * @param c1 Wether or not phase one crashed in this scenario.
    * @param es2 The effects of the second phase of the scenario.
    * @param c2 Wether or not phase two crashed in this scenario.
    * @param es3 The effects of the third phase of the scenario.
    */
  def get (es1: Effects, c1: Boolean, es2: Effects, c2: Boolean, es3: Effects): Counts =
    if (es3.isEmpty)
      Seq.empty
    else
      _get (_phases (es1, c1, es2, c2, es3))

  /** Add the counts for a three phase scenario.
    * @param es1 The effects of the first phase of the scenario.
    * @param c1 Wether or not phase one crashed in this scenario.
    * @param es2 The effects of the second phase of the scenario.
    * @param c2 Wether or not phase two crashed in this scenario.
    * @param es3 The effects of the third phase of the scenario.
    * @param cs3 The counts for each effect of phase two of the scenario.
    */
  def add (es1: Effects, c1: Boolean, es2: Effects, c2: Boolean, es3: Effects, cs3: Counts): Unit =
    _add (_phases (es1, c1, es2, c2, es3), cs3)
}
