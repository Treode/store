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

import scala.util.Random

import com.treode.async.{Async, Scheduler}, Async.supply
import com.treode.async.stubs.{AsyncChecks, CallbackCaptor, StubScheduler}
import com.treode.async.stubs.implicits._
import com.treode.disk.{Disk, DiskLaunch, DiskRecovery}
import com.treode.disk.stubs.StubDiskDrive
import org.scalatest.{Informing, Suite}

trait StubDiskChecks extends AsyncChecks {
  this: Suite with Informing =>

  private def target (n: Int): Int =
    Random.nextInt (n)

  /** An abstract strategy to test a structure. Scenarios run in multiple phases, that is they
    * recover and launch the disk system multiple times. Some scenarios will end a phase
    * prematurely to simulate a crash. Trackers may hook into recovery and launch to check
    * consistency of their structures across simulated restarts, and they may hook into verify
    * to check consistency after all phases.
    */
  trait Tracker {

    /** The type of medic that recovers the structure. */
    type Medic

    /** The type of the structure under test. */
    type Struct

    /** Invoked on each recovery of the structure under test. The tracker should register replay
      * methods and setup a medic, which is an object to recover the structure under test.
      * @return The medic to recover the structure.
      */
    def recover () (implicit scheduler: Scheduler, recovery: DiskRecovery): Medic

    /** Invoked on each launch of the structure under test. The tracker should close out the medic,
      * register checkpointers and compactors, and setup the strucutre under test.
      * @param medic The medic to recover the structure.
      * @return The structure under test.
      */
    def launch (medic: Medic) (implicit scheduler: Scheduler, launch: DiskLaunch): Async [Struct]

    /** Invoked at the end of the scenario. The tracker should check that the structure is
      * consistent with records the tracker has kept across the scenario.
      * @param crashed True if any phase simulated a crash.
      * @param struct The structure under test.
      */
    def verify (crashed: Boolean, struct: Struct) (implicit scheduler: Scheduler): Async [Unit]
  }

  /** An abstract command (see [[http://en.wikipedia.org/wiki/Command_pattern Command Pattern]] to
    * to run during a scenario. StubDiskChecks introduces effects for checkpointing and draining.
    * Component tests add effects for loading the component.
    */
  abstract class Effect [-T <: Tracker] {

    def start (
      tracker: T,
      struct: T#Struct
    ) (implicit
      random: Random,
      scheduler: Scheduler,
      disk: StubDisk
    ): Async [Unit]
  }

  /** Checkpoint `count` times. */
  case class Checkpoint (count: Int) extends Effect [Tracker] {

    def start (
      tracker: Tracker,
      struct: Tracker#Struct
    ) (implicit
      random: Random,
      scheduler: Scheduler,
      disk: StubDisk
    ): Async [Unit] = {
      var i = 0
      scheduler.whilst (i < count) {
        i += 1
        disk.checkpoint()
      }}

    override def toString = s"Checkpoint ($count)"
  }

  /** Drain some random generations. */
  case object Drain extends Effect [Tracker] {

    def start (
      tracker: Tracker,
      struct: Tracker#Struct
    ) (implicit
      random: Random,
      scheduler: Scheduler,
      disk: StubDisk
    ): Async [Unit] =
      supply (disk.drain())

    override def toString = "Drain"
  }

  /** A recover and launch phase of the structure under test. */
  private def phase [T <: Tracker] (
    tracker: T,
    drive: StubDiskDrive,
    lastCrashed: Boolean,
    effects: Seq [(Effect [T], Int)]
  ) (implicit
    random: Random
  ): (Int, Boolean) = {
    implicit val scheduler = StubScheduler.random (random)
    implicit val recovery = StubDisk.recover()
    val medic = tracker.recover()
    implicit val launch = recovery.reattach (drive) .expectPass() .asInstanceOf [StubLaunchAgent]
    implicit val disk = launch.disk.asInstanceOf [StubDisk]
    val struct = tracker.launch (medic) .expectPass()
    launch.launch (lastCrashed)
    var cbs = Seq.empty [CallbackCaptor [Unit]]
    var count = 0
    var crashed = true
    for ((effect, target) <- effects) {
      cbs :+= effect.start (tracker, struct) .capture()
      count = scheduler.run (count = target)
      crashed = (target != Int.MaxValue)
      if (crashed && count != target)
        fail (s"Failed to crash $effect, only $count of $target steps ran.")
    }
    if (!crashed)
      for ((cb, (effect, target)) <- cbs zip effects)
        if (cb.hasFailed [Throwable])
          fail (s"$effect failed: ${cb.assertFailed [Throwable]}")
        else if (!cb.hasPassed)
          fail (s"$effect did not complete $cb")
    (count, crashed)
  }

  /** The final verify phase of the system under test. */
  private def verify [T <: Tracker] (
    tracker: T,
    drive: StubDiskDrive,
    crashed: Boolean
  ) (implicit
    random: Random
  ) {
    implicit val scheduler = StubScheduler.random (random)
    implicit val recovery = StubDisk.recover()
    val medic = tracker.recover()
    implicit val launch = recovery.reattach (drive) .expectPass() .asInstanceOf [StubLaunchAgent]
    implicit val disk = launch.disk.asInstanceOf [StubDisk]
    val struct = tracker.launch (medic) .expectPass()
    launch.launch (crashed)
    tracker.verify (crashed, struct) .expectPass()
  }

  /** Run a one phase scenario---two phases if you count the verify phase. For each effect, this
    * starts the effect, runs the scheduler a given number of steps, and then starts the next
    * effect.
    */
  def onePhase [T <: Tracker] (
    tracker: T,
    seed: Long
  ) (
    effects: (Effect [T], Int)*
  ): Int =
    try {
      implicit val random = new Random (seed)
      implicit val drive = new StubDiskDrive
      val (count, crashed) = phase (tracker, drive, false, effects)
      verify (tracker, drive, crashed)
      if (count > 0) random.nextInt (count) else 0
    } catch {
      case t: Throwable =>
        info (f"onePhase ($tracker, 0x$seed%XL) (${effects mkString ", "})")
        throw t
    }

  /** Run a one phase scenario---two phases if you count the verify phase.
    *
    * This looks up the step counts for `effects.init`. It adds `effect.last` with max steps to
    * simulate completing without a crash. This provides a count of steps available after the last
    * effect has started, which is the upper bound on steps for crashing or starting a subsequent
    * effect.
    *
    * With that upper bound available, this then chooses a number of steps to run the final
    * effect. It uses that count to simulate a crash, and it updates the counter for later
    * scenarios to augment.
    */
  private def onePhase [T <: Tracker] (
    setup: => T,
    seed: Long,
    counter: Counter [Effect [T]]
  ) (
    effects: Effect [T]*
  ): Int = {
    val init = effects.init
    val last = effects.last
    val cs = counter.get (init)
    val t = onePhase (setup, seed) (cs :+ last -> Int.MaxValue : _*)
    val ds = cs :+ last -> t
    counter.add (effects, ds)
    onePhase (setup, seed) (ds : _*)
  }

  /** Run a two phase scenario---three phases if you count the verify phase. For each effect, this
    * starts the effect, runs the scheduler a given number of steps, and then starts the next
    * effect.
    */
  def twoPhases [T <: Tracker] (
    tracker: T,
    seed: Long
  ) (
    first: (Effect [T], Int)*
  ) (
    second: (Effect [T], Int)*
  ): Int =
    try {
      implicit val random = new Random (seed)
      implicit val drive = new StubDiskDrive
      val (count1, crashed1) = phase (tracker, drive, false, first)
      val (count2, crashed2) = phase (tracker, drive, crashed1, second)
      verify (tracker, drive, crashed2)
      if (count2 > 0) random.nextInt (count2) else 0
    } catch {
      case t: Throwable =>
        info (f"twoPhases ($tracker, 0x$seed%XL) (${first mkString ", "}) (${second mkString ", "})")
        throw t
    }

  /** Run a two phase scenario---three phases if you count the verify phase.
    *
    * See the notes for [[#onePhase]] on counting steps between effects, then double the problem.
    */
  private [edit] def twoPhases [T <: Tracker] (
    setup: => T,
    seed: Long,
    counter: Counter [Effect [T]]
  ) (
    first: Effect [T]*
  ) (
    second: Effect [T]*
  ): Int = {
    val last1 = first.last
    val cs1 = counter.get (first)
    val init2 = second.init
    val last2 = second.last

    { // no crash first phase
      val cs2 = counter.get (first, init2, false)
      val ds1 = cs1.init :+ last1 -> Int.MaxValue
      val t = twoPhases (setup, seed) (ds1 : _*) (cs2 :+ last2 -> Int.MaxValue : _*) // no crash second phase
      val ds2 = cs2 :+ last2 -> t
      counter.add (first, second, false, ds1, ds2)
      twoPhases (setup, seed) (ds1 : _*) (ds2 : _*) // crash second phase
    }

    { // crash first phase
      val ds1 = cs1
      val cs2 = counter.get (first, init2, true)
      val t = twoPhases (setup, seed) (ds1 : _*) (cs2 :+ last2 -> Int.MaxValue : _*) // no crash second phase
      val ds2 = cs2 :+ last2 -> t
      counter.add (first, second, true, ds1, ds2)
      twoPhases (setup, seed) (ds1 : _*) (ds2 : _*) // crash second phase
    }}

  /** Run many scenarios with a given PRNG seed. */
  def manyScenarios [T <: Tracker] (
    setup: => T,
    seed: Long,
    phs1: Effect [T]
  ) {
    val counter = new Counter [Effect [T]]

    onePhase (setup, seed, counter) (phs1)
    onePhase (setup, seed, counter) (phs1, Checkpoint (1))
    onePhase (setup, seed, counter) (phs1, Checkpoint (1), Drain)
    onePhase (setup, seed, counter) (phs1, Checkpoint (2))
    onePhase (setup, seed, counter) (phs1, Drain)
    onePhase (setup, seed, counter) (phs1, Drain, Checkpoint (1))

    twoPhases (setup, seed, counter) (phs1) (phs1)
    twoPhases (setup, seed, counter) (phs1, Checkpoint (1)) (phs1)
    twoPhases (setup, seed, counter) (phs1, Checkpoint (1), Drain) (phs1)
    twoPhases (setup, seed, counter) (phs1, Drain) (phs1)
    twoPhases (setup, seed, counter) (phs1, Drain, Checkpoint (1)) (phs1)
  }

  /** Run many scenarios with many PRNG seeds. */
  def manyScenarios [T <: Tracker] (
    setup: => T,
    phs1: Effect [T]
  ): Unit =
    forAllSeeds (manyScenarios (setup, _, phs1))
}
