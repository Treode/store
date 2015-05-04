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

package com.treode.disk.edit

import java.nio.file.{Path, Paths}
import scala.util.Random

import com.treode.async.{Async, Scheduler}, Async.supply
import com.treode.async.implicits._
import com.treode.async.stubs.{AsyncChecks, CallbackCaptor, StubScheduler}
import com.treode.async.stubs.implicits._
import com.treode.disk.{Disk, DiskConfig, DiskLaunch, DriveGeometry, StubFileSystem}
import com.treode.disk.stubs.{Counter, StubDiskEvents}
import org.scalatest.{Informing, Suite}

/** A strategy (see [[http://en.wikipedia.org/wiki/Strategy_pattern Strategy Pattern]]) and
  * scenarios to test components of the live disk system. This tests various scenarios of
  * checkpointing, adding a drive, draining a drive, crashing the system and restarting it. Tests
  * for different components fill in the strategy [[Disks.Tracker]] to load the component and
  * verify its behavior in each scenario.
  */
trait DiskChecks extends AsyncChecks {
  this: Suite with Informing =>

  /** Do a and b overlap? */
  def intersects (a: Set [Path], b: Iterable [Path]): Boolean =
    b exists (a contains _)

  /** Tracks which drives are attached, drained and detached; verifies that attaches, drains and
    * detaches behave correctly; assists in determining which drives to reattach on restart, and
    * which are available to be drained.
    *
    * This tracking and verification is performed for every component to ensure that the component
    * does not foul up the basic attach, drain and detach feature.
    */
  private [edit] class DrivesTracker (implicit files: StubFileSystem, config: DiskConfig) {

    /** Drives that are currenlty attached for certain. */
    private var _attached = Set.empty [Path]

    /** Drives that the test has started to attach. */
    private var _startingAttach = Set.empty [Path]

    /** Drives that the test has started to attach and the async operation completed. */
    private var _finishedAttach = Set.empty [Path]

    /** Drives that the test has started to drain. */
    private var _startingDraining = Set.empty [Path]

    /** Drives that the test has started to and the async operation completed. */
    private var _startedDraining = Set.empty [Path]

    /** Drives that the system under test logged as attached. */
    private var _loggedAttach = Set.empty [Path]

    /** Drives that the system under test logged as detached. */
    private var _loggedDetach = Set.empty [Path]

    /** Drives that the system under test logged as started draining. */
    private var _loggedDraining = Set.empty [Path]

    /** Intercept logging messages. */
    private implicit val events = new StubDiskEvents {

      override def reattachingDisks (paths: Set [Path]) {
        assert (_attached subsetOf paths)
        _attached = paths
      }

      override def changedDisks (attached: Set [Path], detached: Set [Path], draining: Set [Path]) {
        assert (!intersects (attached, _loggedAttach))
        assert (!intersects (detached, _loggedDetach))
        _attached ++= attached
        _attached --= draining
        _loggedAttach ++= attached
        _loggedDetach ++= detached
        _loggedDraining ++= draining
      }}

    /** Drives that are currenlty attached for certain; these are drainable. */
    def attached: Set [Path] =
      _attached

    /** Start recovery using our stub file system and logger. */
    def newRecovery (implicit scheduler: Scheduler): RecoveryAgent =
      new RecoveryAgent

    /** Finish recovery using the files that we are certain were attached. */
    def reattach () (implicit recovery: RecoveryAgent): Async [LaunchAgent] =
      recovery.reattach (_attached.toSeq: _*) .map (_.asInstanceOf [LaunchAgent])

    def startingAttach (paths: Seq [Path], geom: DriveGeometry) {
      files.create (paths, geom.diskBytes.toInt, geom.blockBits)
      _startingAttach ++= paths
    }

    def finishedAttach (paths: Seq [Path]) {
      assert (!intersects (_finishedAttach, paths))
      _finishedAttach ++= paths
    }

    def startingDraining (paths: Seq [Path]): Unit =
      _startingDraining ++= paths

    def startedDraining (paths: Seq [Path]) {
      assert (!intersects (_startedDraining, paths))
      _startedDraining ++= paths
    }

    def verify (crashed: Boolean) (implicit scheduler: Scheduler, agent: DiskAgent): Async [Unit] =
      for {
        digest <- agent.digest
      } yield {
        assert (_attached subsetOf _startingAttach)
        if (crashed) {
          assert (_finishedAttach subsetOf _startingAttach)
          assert (_startedDraining subsetOf _startingDraining)
          assert (_loggedAttach subsetOf _startingAttach)
          assert (_loggedDetach subsetOf _startingDraining)
        } else {
          assert (_finishedAttach == _startingAttach)
          assert (_startedDraining == _startingDraining)
          assert (_loggedAttach == _startingAttach)
          assert (_loggedDetach == _startingDraining)
        }}}

  /** An abstract strategy to test a component. Scenarios run in multiple phases, that is they
    * recover and launch the disk system multiple times. Some scenarios will end a phase
    * prematurely to simulate a crash. Trackers may hook into recovery and launch to check
    * consistency of their components across simulated restarts, and they may hook into verify
    * to check consistency after all phases.
    */
  private [edit] trait Tracker {

    /** Invoked on each recovery of the system under test. */
    def recover () (implicit
      scheduler: Scheduler,
      recovery: RecoveryAgent
    )

    /** Invoked on each launch of the system under test. */
    def launch () (implicit
      random: Random,
      scheduler: Scheduler,
      launch: DiskLaunch
    )

    /** Invoked at the end of the scenario.
      * @param crashed True if any phase simulated a crash.
      */
    def verify (
      crashed: Boolean
    ) (implicit
      scheduler: Scheduler,
      agent: DiskAgent
    ): Async [Unit]
  }

  /** An abstract command (see [[http://en.wikipedia.org/wiki/Command_pattern Command Pattern]] to
    * to run during a scenario. DiskChecks introduces effects for checkpointing, adding disks and
    * draining them. Component tests add effects for loading the component.
    */
  private [edit] abstract class Effect [-T] {

    def start (
      tracker: T
    ) (implicit
      random: Random,
      scheduler: Scheduler,
      agent: DiskAgent,
      drives: DrivesTracker
    ): Async [Unit]
  }

  private [edit] case object Checkpoint extends Effect [Any] {

    def start (
      tracker: Any
    ) (implicit
      random: Random,
      scheduler: Scheduler,
      agent: DiskAgent,
      drives: DrivesTracker
    ): Async [Unit] =
      agent.checkpoint()

    override def toString = "chkpt"
  }

  private [edit] case class AddDisks (name: String, paths: Path*) (implicit geom: DriveGeometry)
  extends Effect [Any] {

    def start (
      tracker: Any
    ) (implicit
      random: Random,
      scheduler: Scheduler,
      agent: DiskAgent,
      drives: DrivesTracker
    ): Async [Unit] = {
      drives.startingAttach (paths, geom)
      paths.latch (agent.attach (_, geom)) .map (_ => drives.finishedAttach (paths))
    }

    override def toString = name
  }

  private [edit] case class RemoveDisks (name: String, paths: Path*) extends Effect [Any] {

    def start (
      tracker: Any
    ) (implicit
      random: Random,
      scheduler: Scheduler,
      agent: DiskAgent,
      drives: DrivesTracker
    ): Async [Unit] = {
      val drainable = drives.attached
      val drains = paths filter (drainable contains _)
      if (drains.size == drainable.size) {
        supply (())
      } else {
        drives.startingDraining (drains)
        drains.latch (agent.drain (_)) .map (_ => drives.startedDraining (drains))
      }}

    override def toString = name
  }

  /** A recover and launch phase of the system under test. */
  private def phase [T <: Tracker] (
    tracker: T,
    effects: Seq [(Effect [T], Int)]
  ) (implicit
    random: Random,
    files: StubFileSystem,
    drives: DrivesTracker
  ): (Int, Boolean) = {
    implicit val scheduler = StubScheduler.random (random)
    implicit val recovery = drives.newRecovery
    tracker.recover()
    implicit val launch = drives.reattach().expectPass()
    import launch.agent
    tracker.launch()
    launch.launch()
    var cbs = Seq.empty [CallbackCaptor [Unit]]
    var count = 0
    var crashed = true
    for ((effect, target) <- effects) {
      cbs :+= effect.start (tracker) .capture()
      count = scheduler.run (count = target)
      crashed = (target != Int.MaxValue)
      if (crashed && count != target)
        fail (s"Failed to crash $effect, only $count of $target steps ran.")
    }
    if (!crashed)
      for ((cb, (effect, target)) <- cbs zip effects)
        if (cb.hasFailed [Throwable])
          throw cb.assertFailed [Throwable]
        else if (!cb.hasPassed)
          fail (s"$effect did not complete $cb")
    (count, crashed)
  }

  /** The final verify phase of the system under test. */
  private def verify (
    tracker: Tracker,
    drives: DrivesTracker,
    crashed: Boolean
  ) (implicit
    random: Random,
    files: StubFileSystem
  ) {
    implicit val scheduler = StubScheduler.random (random)
    implicit val recovery = drives.newRecovery
    tracker.recover()
    implicit val launch = drives.reattach().expectPass()
    import launch.agent
    tracker.launch()
    launch.launch()
    scheduler.run()
    drives.verify (crashed) .expectPass()
    tracker.verify (crashed) .expectPass()
  }

  /** Run a one phase scenario---two phases if you count the verify phase. For each effect, this
    * starts the effect, runs the scheduler a given number of steps, and then starts the next
    * effect.
    */
  private [edit] def onePhase [T <: Tracker] (
    tracker: T,
    seed: Long
  ) (
    effects: (Effect [T], Int)*
  ) (implicit
    config: DiskConfig
  ): Int =
    try {
      implicit val random = new Random (seed)
      implicit val files = new StubFileSystem
      implicit val drives = new DrivesTracker
      val (count, crashed) = phase (tracker, effects)
      verify (tracker, drives, crashed)
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
  private [edit] def onePhase [T <: Tracker] (
    setup: => T,
    seed: Long,
    counter: Counter [Effect [T]]
  ) (
    effects: Effect [T]*
  ) (implicit
    config: DiskConfig
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
  private [edit] def twoPhases [T <: Tracker] (
    tracker: T,
    seed: Long
  ) (
    first: (Effect [T], Int)*
  ) (
    second: (Effect [T], Int)*
  ) (implicit
    config: DiskConfig
  ): Int =
    try {
      implicit val random = new Random (seed)
      implicit val files = new StubFileSystem
      implicit val drives = new DrivesTracker
      val (count1, crashed1) = phase (tracker, first)
      if (!drives.attached.isEmpty) {
        val (count2, crashed2) = phase (tracker, second)
        verify (tracker, drives, crashed1 || crashed2)
        if (count2 > 0) random.nextInt (count2) else 0
      } else {
        0
      }

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
  ) (implicit
    config: DiskConfig
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

  val Seq (da1, da2, da3) = Seq ("da1", "da2", "da3") map (Paths.get (_))
  val Seq (db1, db2, db3) = Seq ("db1", "db2", "db3") map (Paths.get (_))
  val Seq (dc1, dc2, dc3) = Seq ("dc1", "dc2", "dc3") map (Paths.get (_))

  def addA1 (implicit geom: DriveGeometry) = AddDisks ("addA1", da1)
  def addA2 (implicit geom: DriveGeometry) = AddDisks ("addA2", da1, da2)
  def addA3 (implicit geom: DriveGeometry) = AddDisks ("addA3", da1, da2, da3)
  def addB1 (implicit geom: DriveGeometry) = AddDisks ("addB1", db1)
  def addB2 (implicit geom: DriveGeometry) = AddDisks ("addB2", db1, db2)
  def addB3 (implicit geom: DriveGeometry) = AddDisks ("addB3", db1, db2, db3)
  def addC1 (implicit geom: DriveGeometry) = AddDisks ("addC1", dc1)
  def addC2 (implicit geom: DriveGeometry) = AddDisks ("addC2", dc1, dc2)
  def addC3 (implicit geom: DriveGeometry) = AddDisks ("addC3", dc1, dc2, dc3)
  val drnA1 = RemoveDisks ("drnA1", da1)
  val drnA2 = RemoveDisks ("drnA2", da1, da2)
  val drnA3 = RemoveDisks ("drnA3", da1, da2, da3)
  val chkpt = Checkpoint

  /** Run one phase scenarios with a given PRNG seed. */
  private [edit] def onePhaseScenarios [T <: Tracker] (
    setup: => T,
    seed: Long,
    phs1: Effect [T]
  ) (implicit
    config: DiskConfig,
    geom: DriveGeometry
  ) {
    try {
      val counter = new Counter [Effect [T]]

      onePhase (setup, seed, counter) (addA1)
      onePhase (setup, seed, counter) (addA1, phs1)
      onePhase (setup, seed, counter) (addA1, phs1, chkpt)
      onePhase (setup, seed, counter) (addA1, phs1, chkpt, addB1)
      onePhase (setup, seed, counter) (addA1, phs1, addB1)
      onePhase (setup, seed, counter) (addA1, phs1, addB1, chkpt)
      onePhase (setup, seed, counter) (addA1, phs1, addB1, drnA1)
      onePhase (setup, seed, counter) (addA1, phs1, addB1, addC1)
    } catch {
      case t: Throwable =>
        info (f"implicit val geom = $geom")
        throw t
    }}

  /** Run one phase scenarios with many PRNG seeds. */
  private [edit] def onePhaseScenarios [T <: Tracker] (
    setup: => T,
    phs1: Effect [T]
  ) (implicit
    config: DiskConfig,
    geom: DriveGeometry
  ): Unit =
    forAllSeeds (someScenarios (setup, _, phs1))

  /** Run some scenarios with a given PRNG seed. */
  private [edit] def someScenarios [T <: Tracker] (
    setup: => T,
    seed: Long,
    phs1: Effect [T]
  ) (implicit
    config: DiskConfig,
    geom: DriveGeometry
  ) {
    try {
      val counter = new Counter [Effect [T]]

      onePhase (setup, seed, counter) (addA1)
      onePhase (setup, seed, counter) (addA1, phs1)
      onePhase (setup, seed, counter) (addA1, phs1, chkpt)
      onePhase (setup, seed, counter) (addA1, phs1, chkpt, addB1)
      onePhase (setup, seed, counter) (addA1, phs1, addB1)
      onePhase (setup, seed, counter) (addA1, phs1, addB1, chkpt)
      onePhase (setup, seed, counter) (addA1, phs1, addB1, drnA1)
      onePhase (setup, seed, counter) (addA1, phs1, addB1, addC1)

      twoPhases (setup, seed, counter) (addA1) (phs1)
      twoPhases (setup, seed, counter) (addA1, phs1) (phs1)
      twoPhases (setup, seed, counter) (addA1, phs1, chkpt) (phs1)
      twoPhases (setup, seed, counter) (addA1, phs1, addB1) (phs1)
      twoPhases (setup, seed, counter) (addA1, phs1, addB1, drnA1) (phs1)
      twoPhases (setup, seed, counter) (addA1, phs1, addB1) (phs1, drnA1)
    } catch {
      case t: Throwable =>
        info (f"implicit val geom = $geom")
        throw t
    }}

  /** Run some scenarios with many PRNG seeds. */
  private [edit] def someScenarios [T <: Tracker] (
    setup: => T,
    phs1: Effect [T]
  ) (implicit
    config: DiskConfig,
    geom: DriveGeometry
  ): Unit =
    forAllSeeds (someScenarios (setup, _, phs1))

  /** Run many scenarios with a given PRNG seed. */
  private [edit] def manyScenarios [T <: Tracker] (
    setup: => T,
    seed: Long,
    phs1: Effect [T]
  ) (implicit
    config: DiskConfig,
    geom: DriveGeometry
  ) {
    try {
      val counter = new Counter [Effect [T]]

      onePhase (setup, seed, counter) (addA1)
      onePhase (setup, seed, counter) (addA1, phs1)
      onePhase (setup, seed, counter) (addA1, phs1, chkpt)
      onePhase (setup, seed, counter) (addA1, phs1, chkpt, addB1)
      onePhase (setup, seed, counter) (addA1, phs1, addB1)
      onePhase (setup, seed, counter) (addA1, phs1, addB1, chkpt)
      onePhase (setup, seed, counter) (addA1, phs1, addB1, drnA1)
      onePhase (setup, seed, counter) (addA1, phs1, addB1, addC1)
      onePhase (setup, seed, counter) (addA1, phs1, addB2)
      onePhase (setup, seed, counter) (addA1, phs1, addB3)

      onePhase (setup, seed, counter) (addA2)
      onePhase (setup, seed, counter) (addA2, phs1)
      onePhase (setup, seed, counter) (addA2, phs1, addB1)
      onePhase (setup, seed, counter) (addA2, phs1, addB1, drnA1)
      onePhase (setup, seed, counter) (addA2, phs1, drnA1)
      onePhase (setup, seed, counter) (addA2, phs1, drnA1, addB1)

      onePhase (setup, seed, counter) (addA3)
      onePhase (setup, seed, counter) (addA3, phs1)
      onePhase (setup, seed, counter) (addA3, phs1, drnA1)
      onePhase (setup, seed, counter) (addA3, phs1, drnA2)

      twoPhases (setup, seed, counter) (addA1) (phs1)
      twoPhases (setup, seed, counter) (addA1, phs1) (phs1)
      twoPhases (setup, seed, counter) (addA1, phs1, chkpt) (phs1)
      twoPhases (setup, seed, counter) (addA1, phs1, addB1) (phs1)
      twoPhases (setup, seed, counter) (addA1, phs1, addB1, drnA1) (phs1)
      twoPhases (setup, seed, counter) (addA1, phs1, addB1) (phs1, drnA1)

      twoPhases (setup, seed, counter) (addA2, phs1) (phs1)
      twoPhases (setup, seed, counter) (addA2) (phs1)
      twoPhases (setup, seed, counter) (addA2, phs1, drnA1) (phs1)
      twoPhases (setup, seed, counter) (addA2, phs1) (phs1, drnA1)
    } catch {
      case t: Throwable =>
        info (f"implicit val config = $config")
        info (f"implicit val geom = $geom")
        throw t
    }}

  /** Run many scenarios with many PRNG seeds. */
  private [edit] def manyScenarios [T <: Tracker] (
    setup: => T,
    phs1: Effect [T]
  ) (implicit
    config: DiskConfig,
    geom: DriveGeometry
  ): Unit =
    forAllSeeds (manyScenarios (setup, _, phs1))
}
