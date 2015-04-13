package com.treode.disk.edit

import java.nio.file.{Path, Paths}
import scala.util.Random

import com.treode.async.{Async, Scheduler}, Async.supply
import com.treode.disk.{DiskConfig, DiskLaunch, DiskTestConfig, DriveGeometry, RecordDescriptor}
import com.treode.disk.stubs.Counter
import com.treode.pickle.Picklers
import org.scalatest.FreeSpec

class LogSpec extends FreeSpec with DiskChecks {

  val desc = {
    import Picklers._
    RecordDescriptor (0x2A, tuple (int, int))
  }

  private class LogTracker extends Tracker {

    /** The number that we most recently enqueued for logging; incremented each time. */
    private var queued = 0

    /** The maximum number that has been achknowledged by the log thus far. */
    private var flushed = 0

    /** Numbers that we have enqueued, but not acknowledged; they might be replayed. */
    private var recording = Set.empty [Int]

    /** Numbers which have been enqueued and acknowledged; they should be replayed. */
    private var recorded = Set.empty [Int]

    /** Numbers which have been recently checkpointed; they might be replayed. */
    private var checkpointed = Set.empty [Int]

    /** The maximum flushed number that has been replayed thus far. */
    private var replayed = Set.empty [Int]

    def recover () (implicit
      scheduler: Scheduler,
      recovery: RecoveryAgent
    ) {
      flushed = 0
      replayed = Set.empty
      recovery.replay (desc) { case (q, f) =>
        synchronized {
          // Ensure that what is replayed now was enqueued after what has been replayed already.
          replayed += q
          assert (flushed < q)
          if (flushed < f)
            flushed = f
        }}}

    def launch () (implicit
      random: Random,
      scheduler: Scheduler,
      launch: DiskLaunch
    ) {
      // Ensure that what was recorded has been replayed or checkpointed.
      assert (recorded subsetOf (replayed ++ checkpointed))
      recorded = replayed
      launch.checkpoint {
        checkpointed = recording ++ recorded
        recording = Set.empty
        recorded = Set.empty
        supply (())
      }}

    /** Log a record; track what we are recording and what has been recorded. */
    def record () (implicit
      scheduler: Scheduler,
      agent: DiskAgent
    ): Async [Unit] = {
      val (q, f) = synchronized {
        queued += 1
        (queued, flushed)
      }
      recording += q
      agent.record (desc, (q, f)) .map { _ =>
        synchronized {
          recording -= q
          recorded += q
          if (flushed < q)
            flushed = q
        }}}

    /** Log nbatches of nrecords. */
    def record (
      nbatches: Int,
      nwrites: Int
    ) (implicit
      random: Random,
      scheduler: Scheduler,
      agent: DiskAgent
    ): Async [Unit] = {
      var i = 0
      scheduler.whilst (i < nbatches) {
        i += 1
        Async.count (nwrites) (record())
      }}

    def verify (
      crashed: Boolean
    ) (implicit
      scheduler: Scheduler,
      agent: DiskAgent
    ): Async [Unit] =
      supply {
        // Assertions made in launch.
      }

    override def toString = s"new LogTracker"
  }

  private case class LogBatch (nbatches: Int, nwrites: Int) extends Effect [LogTracker] {

    def start (
      tracker: LogTracker
    ) (implicit
      random: Random,
      scheduler: Scheduler,
      agent: DiskAgent,
      drives: DrivesTracker
    ): Async [Unit] =
      tracker.record (nbatches, nwrites)

    override def toString = s"LogBatch ($nbatches, $nwrites)"
  }

  // TODO: Gradually add logging features until we can run manyScenarios from DiskChecks.
  private [edit] def logScenarios [T <: Tracker] (
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
    } catch {
      case t: Throwable =>
        info (f"implicit val config = $config")
        info (f"implicit val geom = $geom")
        throw t
    }}

  private [edit] def logScenarios [T <: Tracker] (
    setup: => T,
    phs1: Effect [T]
  ) (implicit
    config: DiskConfig,
    geom: DriveGeometry
  ): Unit =
    forAllSeeds (logScenarios (setup, _, phs1))

  "The Log should replay acknowledged records in semi-order" - {

    // TODO: Gradually add logging features until we can record more.
    for {
      nbatches <- Seq (1, 2) // Seq (1, 2, 3)
      nwrites <- Seq (1, 2) // Seq (1, 2, 3)
      if (nbatches != 0 && nwrites != 0 || nbatches == nwrites)
    } s"for $nbatches batches of $nwrites writes" in {
      implicit val config = DiskTestConfig()
      implicit val geom = DriveGeometry (8, 6, 1 << 18)
      logScenarios (new LogTracker, LogBatch (nbatches, nwrites))
    }

    /*for {
      (nbatches, nwrites) <- Seq ((7, 7), (16, 16))
    } s"for $nbatches batches of $nwrites writes" in {
      implicit val config = DiskTestConfig()
      implicit val geom = DriveGeometry (8, 6, 1 << 18)
      logScenarios (new LogTracker, LogBatch (nbatches, nwrites))
    }

    for {
      (nbatches, nwrites) <- Seq ((20, 100))
    } s"for $nbatches batches of $nwrites writes" in {
      implicit val config = DiskTestConfig()
      implicit val geom = DriveGeometry (8, 6, 1 << 18)
      logScenarios (new LogTracker, LogBatch (nbatches, nwrites))
    }*/}}
