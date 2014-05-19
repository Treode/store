package com.treode.store

import java.util.concurrent.Executors
import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.async.stubs.{AsyncChecks, CallbackCaptor, StubScheduler}
import com.treode.async.stubs.implicits._
import com.treode.cluster.HostId
import com.treode.cluster.stubs.StubNetwork
import com.treode.disk.stubs.StubDiskDrive
import org.scalatest.{Assertions, Informing, Suite}

import Async.async
import StoreClusterChecks.{Host, Package}
import StoreTestTools._

trait StoreClusterChecks extends AsyncChecks {
  this: Suite with Informing =>

  private val ntargets =
    intensity match {
      case "development" => 1
      case _ => 10
    }

  private val nthreads =
    if (Runtime.getRuntime.availableProcessors < 8) 4 else 8

  val H1 = 0xF7DD0B042DACCD44L
  val H2 = 0x3D74427D18B38275L
  val H3 = 0x1FC96D277C03FEC3L

  class ForStoreClusterRunner [H] (
      val messages: Seq [String],
      val pkg: Package [H],
      val setup: Scheduler => (H, H) => Async [_],
      val asserts: Seq [Unit => Unit],
      val recover: Scheduler => H => Async [_]
  ) {

    def boot (
        id: HostId,
        checkpoint: Double,
        compaction: Double,
        drive: StubDiskDrive,
        init: Boolean
    ) (implicit
        random: Random,
        parent: Scheduler,
        network: StubNetwork
    ): Async [H] =
      pkg.boot (id, checkpoint, compaction, drive, init)

    def install (
        id: HostId,
        checkpoint: Double,
        compaction: Double
    ) (implicit
        random: Random,
        parent: Scheduler,
        network: StubNetwork
    ): Async [H] =
      pkg.boot (id, checkpoint, compaction, new StubDiskDrive, true)
  }

  class ForStoreClusterRecover [H] (
      messages: Seq [String],
      pkg: Package [H],
      setup: Scheduler => (H, H) => Async [_]
  ) {

    private val asserts = Seq.newBuilder [Unit => Unit]

    def assert (cond: => Boolean, msg: String): ForStoreClusterRecover [H] = {
      asserts += (_ => Assertions.assert (cond, msg))
      this
    }

    def recover (recover: Scheduler => H => Async [_]) =
      new ForStoreClusterRunner (messages, pkg, setup, asserts.result, recover)
  }

  class ForStoreClusterSetup [H] (
      messages: Seq [String],
      pkg: Package [H]
  ) {

    def setup (setup: Scheduler => (H, H) => Async [_]) =
      new ForStoreClusterRecover (messages, pkg, setup)
  }

  class ForStoreClusterHost {

    private val messages = Seq.newBuilder [String]

    def info (msg: String): ForStoreClusterHost = {
      messages += msg
      this
    }

    def host [H] (pkg: Package [H]): ForStoreClusterSetup [H] =
      new ForStoreClusterSetup (messages.result, pkg)
  }

  def cluster: ForStoreClusterHost =
    new ForStoreClusterHost

  private class StubSchedulerKit [H] (
      val runner: ForStoreClusterRunner [H]
   ) (implicit
      val random: Random,
      val scheduler: StubScheduler,
      val network: StubNetwork
  )

  private class MultithreadedSchedulerKit [H] (
      val runner: ForStoreClusterRunner [H]
   ) (implicit
      val random: Random,
      val scheduler: Scheduler,
      val network: StubNetwork
  )

  private implicit class NamedTest (name: String) {

    def withStubScheduler [H, A] (
        seed: Long,
        init: Random => ForStoreClusterRunner [H]
    ) (
        test: StubSchedulerKit [H] => A
    ): A = {
      implicit val random = new Random (seed)
      val runner = init (random)
      try {
        implicit val scheduler = StubScheduler.random (random)
        implicit val network = StubNetwork (random)
        test (new StubSchedulerKit (runner))
      } catch {
        case t: Throwable =>
          info (name)
          runner.messages foreach (info (_))
          throw t
      }}

    def withMultithreadedScheduler [H, A] (
        init: Random => ForStoreClusterRunner [H]
    ) (
        test: MultithreadedSchedulerKit [H] => A
    ): A = {
      implicit val random = Random
      val executor = Executors.newScheduledThreadPool (nthreads)
      val runner = init (random)
      try {
        implicit val scheduler = Scheduler (executor)
        implicit val network = StubNetwork (random)
        test (new MultithreadedSchedulerKit (runner))
      } catch {
        case t: Throwable =>
          info (name)
          runner.messages foreach (info (_))
          throw t
      } finally {
        executor.shutdown()
      }}}

  private def forSeeds (test: Long => Any): Long = {
    val start = System.currentTimeMillis
    for (_ <- 0 until nseeds)
      test (Random.nextLong())
    val end = System.currentTimeMillis
    (end - start) / nseeds
  }

  private def forTargets (count: Long => Int) (test: (Long, Int) => Any): Long = {
    val start = System.currentTimeMillis
    var nruns = 0
    for (_ <- 0 until nseeds / ntargets) {
      val seed = Random.nextLong()
      val max = count (seed)
      nruns += 1
      if (max < ntargets) {
        for (i <- 1 to max)
          test (seed, i)
        nruns += max
      } else {
        for (i <- 1 to ntargets)
          test (seed, Random.nextInt (max - 1) + 1)
        nruns += ntargets
      }}
    val end = System.currentTimeMillis()
    (start - end) / nruns
  }

  private def forDoubleTargets (
      count1: Long => Int
  ) (
      count2: (Long, Int) => Int
  ) (
      test: (Long, Int, Int) => Any
  ): Long = {
    val start = System.currentTimeMillis
    var nruns = 0
    for (_ <- 0 until nseeds / ntargets) {
      val seed = Random.nextLong()
      val max1 = count1 (seed)
      nruns += 1
      if (max1 < ntargets) {
        for (i <- 1 to max1) {
          val max2 = count2 (seed, i)
          test (seed, i, Random.nextInt (max2 - 1) + 1)
        }
        nruns += max1 + max1
      } else {
        for (i <- 1 to ntargets) {
          val t1 = Random.nextInt (max1 - 1) + 1
          val max2 = count2 (seed, t1)
          test (seed, t1, Random.nextInt (max2 - 1) + 1)
        }
        nruns += ntargets + ntargets
      }}
    val end = System.currentTimeMillis()
    (start - end) / nruns
  }

  def forOneHost [H <: Host] (
      seed: Long,
      checkpoint: Double,
      compaction: Double,
      flakiness: Double
  ) (
      init: Random => ForStoreClusterRunner [H]
  ): Int =

    s"forOneHost (${seed}L, $checkpoint, $compaction, $flakiness)"
    .withStubScheduler (seed, init) { kit =>
      import kit._

      val h1 = runner .install (H1, checkpoint, compaction) .pass
      for (h <- Seq (h1))
        h.setAtlas (settled (h1))

      network.messageFlakiness = flakiness
      val cb = runner.setup (scheduler) (h1, h1) .capture()
      val count = scheduler.run (timers = !cb.wasInvoked)
      cb.passedOrTimedout
      runner.asserts foreach (_ ())
      network.messageFlakiness = 0.0

      runner.recover (scheduler) (h1) .pass

      count
    }

  def forOneHost [H <: Host] (
      checkpoint: Double,
      compaction: Double,
      flakiness: Double
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) {
    val average = forSeeds (forOneHost (_, checkpoint, compaction, flakiness) (init))
    info (s"Average time on one host: ${average}ms")
  }

  def forThreeStableHosts [H <: Host] (
      seed: Long,
      checkpoint: Double,
      compaction: Double,
      flakiness: Double
  ) (
      init: Random => ForStoreClusterRunner [H]
  ): Int =

    s"forThreeStableHosts (${seed}L, $checkpoint, $compaction, $flakiness)"
    .withStubScheduler (seed, init) { kit =>
      import kit._

      val h1 = runner .install (H1, checkpoint, compaction) .pass
      val h2 = runner .install (H2, checkpoint, compaction) .pass
      val h3 = runner .install (H3, checkpoint, compaction) .pass
      for (h <- Seq (h1, h2, h3))
        h.setAtlas (settled (h1, h2, h3))

      network.messageFlakiness = flakiness
      val cb = runner.setup (scheduler) (h1, h2) .capture()
      val count = scheduler.run (timers = !cb.wasInvoked)
      cb.passedOrTimedout
      runner.asserts foreach (_ ())
      network.messageFlakiness = 0.0

      runner.recover (scheduler) (h1) .pass

      count
    }

  def forThreeStableHosts [H <: Host] (
      checkpoint: Double,
      compaction: Double,
      flakiness: Double
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) {
    val average = forSeeds (forThreeStableHosts (_, checkpoint, compaction, flakiness) (init))
    info (s"Average time on three stable hosts: ${average}ms")
  }

  def forThreeStableHostsMultithreaded [H <: Host] (
      checkpoint: Double,
      compaction: Double,
      flakiness: Double
  ) (
      init: Random => ForStoreClusterRunner [H]
  ): Int =

    s"forThreeStableHostsMultithreaded ($checkpoint, $compaction, $flakiness)"
    .withMultithreadedScheduler (init) { kit =>
      import kit._

      val h1 = runner .install (H1, checkpoint, compaction) .await()
      val h2 = runner .install (H2, checkpoint, compaction) .await()
      val h3 = runner .install (H3, checkpoint, compaction) .await()
      for (h <- Seq (h1, h2, h3))
        h.setAtlas (settled (h1, h2, h3))

      network.messageFlakiness = flakiness
      val start = System.currentTimeMillis
      runner.setup (scheduler) (h1, h2) .passOrTimeout
      val end = System.currentTimeMillis
      runner.asserts foreach (_ ())
      network.messageFlakiness = 0.0

      runner.recover (scheduler) (h1) .await()

      (end - start).toInt
    }

  def forOneHostOffline [H <: Host] (
      seed: Long,
      checkpoint: Double,
      compaction: Double,
      flakiness: Double
  ) (
      init: Random => ForStoreClusterRunner [H]
  ): Int =

    s"forOneHostOffline (${seed}L, $checkpoint, $compaction, $flakiness)"
    .withStubScheduler (seed, init) { kit =>
      import kit._

      val d1 = new StubDiskDrive
      val d2 = new StubDiskDrive

      var h1 = runner .boot (H1, checkpoint, compaction, d1, true) .pass
      var h2 = runner .boot (H2, checkpoint, compaction, d2, true) .pass
      val h3 = runner .install (H3, checkpoint, compaction) .pass
      h3.shutdown()
      for (h <- Seq (h1, h2, h3))
        h.setAtlas (settled (h1, h2, h3))

      network.messageFlakiness = flakiness
      val cb = runner.setup (scheduler) (h1, h2) .capture()
      val count = scheduler.run (timers = !cb.wasInvoked, oblivious = true)
      cb.passedOrTimedout
      runner.asserts foreach (_ ())
      network.messageFlakiness = 0.0

      h1.shutdown()
      h2.shutdown()
      h1 = runner .boot (H1, checkpoint, compaction, d1, false) .pass
      h2 = runner .boot (H2, checkpoint, compaction, d2, false) .pass
      for (h <- Seq (h1, h2))
        h.setAtlas (settled (h1, h2, h3))
      val cb2 = runner.recover (scheduler) (h1) .capture()
      scheduler.run (timers = !cb2.wasInvoked, oblivious = true)
      cb2.passed

      count
    }

  def forOneHostOffline [H <: Host] (
      checkpoint: Double,
      compaction: Double,
      flakiness: Double
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) {
    val average = forSeeds (forOneHostOffline (_, checkpoint, compaction, flakiness) (init))
    info (s"Average time with one host crashed: ${average}ms")
  }

  def forOneHostOfflineMultithreaded [H <: Host] (
      checkpoint: Double,
      compaction: Double,
      flakiness: Double
  ) (
      init: Random => ForStoreClusterRunner [H]
  ): Int =

    s"forOneHostOfflineMultithreaded ($checkpoint, $compaction, $flakiness)"
    .withMultithreadedScheduler (init) { kit =>
      import kit._

      val d1 = new StubDiskDrive
      val d2 = new StubDiskDrive

      var h1 = runner .boot (H1, checkpoint, compaction, d1, true) .await()
      var h2 = runner .boot (H2, checkpoint, compaction, d2, true) .await()
      val h3 = runner .install (H3, checkpoint, compaction) .await()
      h3.shutdown()
      for (h <- Seq (h1, h2, h3))
        h.setAtlas (settled (h1, h2, h3))

      network.messageFlakiness = flakiness
      val start = System.currentTimeMillis
      runner.setup (scheduler) (h1, h2) .passOrTimeout
      val end = System.currentTimeMillis
      runner.asserts foreach (_ ())
      network.messageFlakiness = 0.0

      h1.shutdown()
      h2.shutdown()
      h1 = runner .boot (H1, checkpoint, compaction, d1, false) .await()
      h2 = runner .boot (H2, checkpoint, compaction, d2, false) .await()
      for (h <- Seq (h1, h2))
        h.setAtlas (settled (h1, h2, h3))
      runner.recover (scheduler) (h1) .await()

      (end - start).toInt
    }

  def forOneHostCrashing [H <: Host] (
      seed: Long,
      target: Int,
      checkpoint: Double,
      compaction: Double,
      flakiness: Double
  ) (
      init: Random => ForStoreClusterRunner [H]
  ): Int =

    s"forOneHostCrashing (${seed}L, $target, $checkpoint, $compaction, $flakiness)"
    .withStubScheduler (seed, init) { kit =>
      import kit._

      val d1 = new StubDiskDrive
      val d2 = new StubDiskDrive
      val d3 = new StubDiskDrive

      var h1 = runner .boot (H1, checkpoint, compaction, d1, true) .pass
      var h2 = runner .boot (H2, checkpoint, compaction, d2, true) .pass
      var h3 = runner .boot (H3, checkpoint, compaction, d3, true) .pass
      for (h <- Seq (h1, h2, h3))
        h.setAtlas (settled (h1, h2, h3))

      network.messageFlakiness = flakiness
      val cb = runner.setup (scheduler) (h1, h2) .capture()
      scheduler.run (count = target, timers = true)
      h3.shutdown()
      val count = scheduler.run (timers = !cb.wasInvoked, oblivious = true)
      cb.passedOrTimedout
      runner.asserts foreach (_ ())
      network.messageFlakiness = 0.0

      h1.shutdown()
      h2.shutdown()
      h1 = runner .boot (H1, checkpoint, compaction, d1, false) .pass
      h2 = runner .boot (H2, checkpoint, compaction, d2, false) .pass
      h3 = runner .boot (H3, checkpoint, compaction, d3, false) .pass
      for (h <- Seq (h1, h2, h3))
        h.setAtlas (settled (h1, h2, h3))
      runner .recover (scheduler) (h1) .pass

      count
    }

  def forOneHostCrashing [H <: Host] (
      checkpoint: Double,
      compaction: Double,
      flakiness: Double
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) {
    val average = forTargets (
        forThreeStableHosts (_, checkpoint, compaction, flakiness) (init)) (
            forOneHostCrashing (_, _, checkpoint, compaction, flakiness) (init))
    info (s"Average time with one host crashing: ${average}ms")
  }

  def forOneHostCrashingMultithreaded [H <: Host] (
      target: Int,
      checkpoint: Double,
      compaction: Double,
      flakiness: Double
  ) (
      init: Random => ForStoreClusterRunner [H]
  ): Int =

    s"forOneHostCrashingMultithreaded ($target, $checkpoint, $compaction, $flakiness)"
    .withMultithreadedScheduler (init) { kit =>
      import kit._

      val d1 = new StubDiskDrive
      val d2 = new StubDiskDrive
      val d3 = new StubDiskDrive

      var h1 = runner .boot (H1, checkpoint, compaction, d1, true) .await()
      var h2 = runner .boot (H2, checkpoint, compaction, d2, true) .await()
      var h3 = runner .boot (H3, checkpoint, compaction, d3, true) .await()
      for (h <- Seq (h1, h2, h3))
        h.setAtlas (settled (h1, h2, h3))

      network.messageFlakiness = flakiness
      scheduler.delay (target) (h3.shutdown())
      val start = System.currentTimeMillis
      runner.setup (scheduler) (h1, h2) .passOrTimeout
      val end = System.currentTimeMillis
      runner.asserts foreach (_ ())
      network.messageFlakiness = 0.0

      h1.shutdown()
      h2.shutdown()
      h3.shutdown()
      h1 = runner .boot (H1, checkpoint, compaction, d1, false) .await()
      h2 = runner .boot (H2, checkpoint, compaction, d2, false) .await()
      h3 = runner .boot (H3, checkpoint, compaction, d3, false) .await()
      for (h <- Seq (h1, h2, h3))
        h.setAtlas (settled (h1, h2, h3))

      runner .recover (scheduler) (h1) .await()

      (end - start).toInt
    }

  def forOneHostCrashingMultithreaded [H <: Host] (
      checkpoint: Double,
      compaction: Double,
      flakiness: Double
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) {
    val time = forThreeStableHostsMultithreaded (checkpoint, compaction, flakiness) (init)
    val target =  Random.nextInt ((time * 0.7).toInt) + (time * 0.1).toInt
    forOneHostCrashingMultithreaded (target, checkpoint, compaction, flakiness) (init)
  }

  def forOneHostRebooting [H <: Host] (
      seed: Long,
      target: Int,
      checkpoint: Double,
      compaction: Double,
      flakiness: Double
  ) (
      init: Random => ForStoreClusterRunner [H]
  ): Unit =

    s"forOneHostRebooting (${seed}L, $target, $checkpoint, $compaction, $flakiness)"
    .withStubScheduler (seed, init) { kit =>
      import kit._

      val d1 = new StubDiskDrive
      val d2 = new StubDiskDrive
      val d3 = new StubDiskDrive

      var h1 = runner .boot (H1, checkpoint, compaction, d1, true) .pass
      var h2 = runner .boot (H2, checkpoint, compaction, d2, true) .pass
      var h3 = runner .boot (H3, checkpoint, compaction, d3, true) .pass
      h3.shutdown()
      for (h <- Seq (h1, h2, h3))
        h.setAtlas (settled (h1, h2, h3))

      network.messageFlakiness = flakiness
      val cb = runner.setup (scheduler) (h1, h2) .capture()
      scheduler.run (count = target, timers = true, oblivious = true)
      val cb2 = runner .boot (H3, checkpoint, compaction, d3, false) .capture()
      scheduler.run (timers = !cb.wasInvoked || !cb2.wasInvoked, oblivious = true)
      cb.passedOrTimedout
      h3 = cb2.passed
      runner.asserts foreach (_ ())
      network.messageFlakiness = 0.0

      h1.shutdown()
      h2.shutdown()
      h3.shutdown()
      h1 = runner .boot (H1, checkpoint, compaction, d1, false) .pass
      h2 = runner .boot (H2, checkpoint, compaction, d2, false) .pass
      h3 = runner .boot (H3, checkpoint, compaction, d3, false) .pass
      for (h <- Seq (h1, h2, h3))
        h.setAtlas (settled (h1, h2, h3))
      runner .recover (scheduler) (h1) .pass
    }

  def forOneHostRebooting [H <: Host] (
      checkpoint: Double,
      compaction: Double,
      flakiness: Double
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) {
    val average = forTargets (
        forOneHostOffline (_, checkpoint, compaction, flakiness) (init)) (
            forOneHostRebooting (_, _, checkpoint, compaction, flakiness) (init))
    info (s"Average time with one host rebooting: ${average}ms")
  }

  def forOneHostRebootingMultithreaded [H <: Host] (
      target: Int,
      checkpoint: Double,
      compaction: Double,
      flakiness: Double
  ) (
      init: Random => ForStoreClusterRunner [H]
  ): Unit =

    s"forOneHostRebootingMultithreaded ($target, $checkpoint, $compaction, $flakiness)"
    .withMultithreadedScheduler (init) { kit =>
      import kit._

      val d1 = new StubDiskDrive
      val d2 = new StubDiskDrive
      val d3 = new StubDiskDrive

      var h1 = runner .boot (H1, checkpoint, compaction, d1, true) .await()
      var h2 = runner .boot (H2, checkpoint, compaction, d2, true) .await()
      var h3 = runner .boot (H3, checkpoint, compaction, d3, true) .await()
      h3.shutdown()
      for (h <- Seq (h1, h2, h3))
        h.setAtlas (settled (h1, h2, h3))

      network.messageFlakiness = flakiness
      val _h3 =
        (for {
          _ <- Async.delay (target)
          h <- runner.boot (H3, checkpoint, compaction, d3, false)
        } yield h) .toFuture
      runner.setup (scheduler) (h1, h2) .passOrTimeout
      runner.asserts foreach (_ ())
      h3 = _h3.await()
      network.messageFlakiness = 0.0

      h1.shutdown()
      h2.shutdown()
      h3.shutdown()
      h1 = runner .boot (H1, checkpoint, compaction, d1, false) .await()
      h2 = runner .boot (H2, checkpoint, compaction, d2, false) .await()
      h3 = runner .boot (H3, checkpoint, compaction, d3, false) .await()
      for (h <- Seq (h1, h2, h3))
        h.setAtlas (settled (h1, h2, h3))
      runner .recover (scheduler) (h1) .await()
    }

  def forOneHostRebootingMultithreaded [H <: Host] (
      checkpoint: Double,
      compaction: Double,
      flakiness: Double
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) {
    val time = forOneHostOfflineMultithreaded (checkpoint, compaction, flakiness) (init)
    val target =  Random.nextInt ((time * 0.7).toInt) + (time * 0.1).toInt
    forOneHostRebootingMultithreaded (target, checkpoint, compaction, flakiness) (init)
  }

  def forOneHostBouncing [H <: Host] (
      seed: Long,
      target1: Int,
      target2: Int,
      checkpoint: Double,
      compaction: Double,
      flakiness: Double
  ) (
      init: Random => ForStoreClusterRunner [H]
  ): Unit =

    s"forOneHostBouncing (${seed}L, $target1, $target2, $checkpoint, $compaction, $flakiness)"
    .withStubScheduler (seed, init) { kit =>
      import kit._

      val d1 = new StubDiskDrive
      val d2 = new StubDiskDrive
      val d3 = new StubDiskDrive

      var h1 = runner .boot (H1, checkpoint, compaction, d1, true) .pass
      var h2 = runner .boot (H2, checkpoint, compaction, d2, true) .pass
      var h3 = runner .boot (H3, checkpoint, compaction, d3, true) .pass
      for (h <- Seq (h1, h2, h3))
        h.setAtlas (settled (h1, h2, h3))

      network.messageFlakiness = flakiness
      val cb = runner.setup (scheduler) (h1, h2) .capture()
      scheduler.run (count = target1, timers = true, oblivious = true)
      h3.shutdown()
      scheduler.run (count = target2, timers = true, oblivious = true)
      val cb2 = runner .boot (H3, checkpoint, compaction, d3, false) .capture()
      scheduler.run (timers = !cb.wasInvoked || !cb2.wasInvoked, oblivious = true)
      cb.passedOrTimedout
      h3 = cb2.passed
      runner.asserts foreach (_ ())
      network.messageFlakiness = 0.0

      h1.shutdown()
      h2.shutdown()
      h3.shutdown()
      h1 = runner .boot (H1, checkpoint, compaction, d1, false) .pass
      h2 = runner .boot (H2, checkpoint, compaction, d2, false) .pass
      h3 = runner .boot (H3, checkpoint, compaction, d3, false) .pass
      for (h <- Seq (h1, h2, h3))
        h.setAtlas (settled (h1, h2, h3))
      runner .recover (scheduler) (h1) .pass
    }

  def forOneHostBouncing [H <: Host] (
      checkpoint: Double,
      compaction: Double,
      flakiness: Double
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) {
    val average = forDoubleTargets (
        forThreeStableHosts (_, checkpoint, compaction, flakiness) (init)) (
            forOneHostCrashing (_, _, checkpoint, compaction, flakiness) (init)) (
                forOneHostBouncing (_, _, _, checkpoint, compaction, flakiness) (init))
    info (s"Average time with one host bouncing: ${average}ms")
  }

  def forOneHostBouncingMultithreaded [H <: Host] (
      target1: Int,
      target2: Int,
      checkpoint: Double,
      compaction: Double,
      flakiness: Double
  ) (
      init: Random => ForStoreClusterRunner [H]
  ): Unit =

    s"forOneHostRebootingMultithreaded ($target1, $target2, $checkpoint, $compaction, $flakiness)"
    .withMultithreadedScheduler (init) { kit =>
      import kit._

      val d1 = new StubDiskDrive
      val d2 = new StubDiskDrive
      val d3 = new StubDiskDrive

      var h1 = runner .boot (H1, checkpoint, compaction, d1, true) .await()
      var h2 = runner .boot (H2, checkpoint, compaction, d2, true) .await()
      var h3 = runner .boot (H3, checkpoint, compaction, d3, true) .await()
      for (h <- Seq (h1, h2, h3))
        h.setAtlas (settled (h1, h2, h3))

      network.messageFlakiness = flakiness
      val _h3 =
        (for {
          _ <- Async.delay (target1)
          _ = h3.shutdown()
          _ <- Async.delay (target2)
          h <- runner .boot (H3, checkpoint, compaction, d3, false)
        } yield h) .toFuture
      runner.setup (scheduler) (h1, h2) .passOrTimeout
      runner.asserts foreach (_ ())
      h3 = _h3.await()
      network.messageFlakiness = 0.0

      h1.shutdown()
      h2.shutdown()
      h3.shutdown()
      h1 = runner .boot (H1, checkpoint, compaction, d1, false) .await()
      h2 = runner .boot (H2, checkpoint, compaction, d2, false) .await()
      h3 = runner .boot (H3, checkpoint, compaction, d3, false) .await()
      for (h <- Seq (h1, h2, h3))
        h.setAtlas (settled (h1, h2, h3))
      runner .recover (scheduler) (h1) .await()
    }

  def forOneHostBouncingMultithreaded [H <: Host] (
      checkpoint: Double,
      compaction: Double,
      flakiness: Double
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) {
    val time1 = forThreeStableHostsMultithreaded (checkpoint, compaction, flakiness) (init)
    val target1 = Random.nextInt ((time1 * 0.7).toInt) + (time1 * 0.1).toInt
    val time2 = forOneHostCrashingMultithreaded (target1, checkpoint, compaction, flakiness) (init)
    val target2 =  Random.nextInt ((time2 * 0.7).toInt) + (time2 * 0.1).toInt
    forOneHostBouncingMultithreaded (target1, target2, checkpoint, compaction, flakiness) (init)
  }}

object StoreClusterChecks {

  trait Host extends StubStoreHost {

    def setAtlas (cohorts: Cohort*)
    def shutdown()
  }

  trait Package [H] {

    def boot (
        id: HostId,
        checkpoint: Double,
        compaction: Double,
        drive: StubDiskDrive,
        init: Boolean
    ) (implicit
        random: Random,
        parent: Scheduler,
        network: StubNetwork
    ): Async [H]
  }}
