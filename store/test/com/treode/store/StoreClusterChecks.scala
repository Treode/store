package com.treode.store

import java.util.concurrent.Executors
import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.async.implicits._
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

  val H1 = 0x44L
  val H2 = 0x75L
  val H3 = 0xC3L
  val H4 = 0x7EL
  val H5 = 0x8CL
  val H6 = 0x32L
  val H7 = 0xEDL
  val H8 = 0xBFL

  class ForStoreClusterRunner [H] (
      val messages: Seq [String],
      val pkg: Package [H],
      val setup: Scheduler => (H, H) => Async [_],
      val recover: Scheduler => H => Async [_]
  ) {

    private val _whilsts = Seq.newBuilder [Seq [H] => Boolean]
    lazy val whilsts = _whilsts.result

    private val _verifications = Seq.newBuilder [StubScheduler => Seq [H] => Async [Unit]]
    lazy val verifications = _verifications.result

    def whilst (cond: Seq [H] => Boolean): ForStoreClusterRunner [H] = {
      _whilsts += cond
      this
    }

    def verify (test: StubScheduler => Seq [H] => Async [Unit]): ForStoreClusterRunner [H] = {
      _verifications += test
      this
    }

    def boot (
        id: HostId,
        drive: StubDiskDrive,
        init: Boolean
    ) (implicit
        random: Random,
        parent: Scheduler,
        network: StubNetwork,
        config: StoreTestConfig
    ): Async [H] =
      pkg.boot (id, drive, init)

    def install (
        id: HostId
    ) (implicit
        random: Random,
        parent: Scheduler,
        network: StubNetwork,
        config: StoreTestConfig
    ): Async [H] =
      pkg.boot (id, new StubDiskDrive, true)

    def verify (hs: Seq [H]) (implicit scheduler: StubScheduler) {
      for (cond <- whilsts)
        scheduler.run (timers = cond (hs))
      for (test <- verifications)
        test (scheduler) (hs) .pass
    }}

  class ForStoreClusterRecover [H] (
      messages: Seq [String],
      pkg: Package [H],
      setup: Scheduler => (H, H) => Async [_]
  ) {

    def recover (recover: Scheduler => H => Async [_]) =
      new ForStoreClusterRunner [H] (messages, pkg, setup, recover)
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
    (end - start) / nruns
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
          if (max2 > 1)
            test (seed, i, Random.nextInt (max2 - 1) + 1)
        }
        nruns += max1 + max1
      } else {
        for (i <- 1 to ntargets) {
          val t1 = Random.nextInt (max1 - 1) + 1
          val max2 = count2 (seed, t1)
          if (max2 > 1)
            test (seed, t1, Random.nextInt (max2 - 1) + 1)
        }
        nruns += ntargets + ntargets
      }}
    val end = System.currentTimeMillis()
    (end - start) / nruns
  }

  private def cohortsFor8 (hs: Seq [StubStoreHost]): Seq [Cohort] = {
    val Seq (h1, h2, h3, h4, h5, h6, h7, h8) = hs
    Seq (
        settled (h1, h2, h6),
        settled (h1, h3, h7),
        settled (h1, h4, h8),
        settled (h2, h3, h8),
        settled (h2, h4, h7),
        settled (h3, h5, h6),
        settled (h4, h5, h8),
        settled (h5, h6, h7))
  }

  private def rewriteFor8 (prev: Seq [Cohort], hosts: Seq [StubStoreHost]): Seq [Cohort] = {
    import Cohort.{Issuing, Moving, Settled}
    for ((Settled (hs1), i) <- cohortsFor8 (hosts) .zipWithIndex) yield {
      prev (i % prev.length) match {
        case Issuing (hs0, _) if hs0 == hs1 =>
          Settled (hs0)
        case Issuing (hs0, _) =>
          Issuing (hs0, hs1)
        case Moving (hs0, _) if hs0 == hs1 =>
          Settled (hs0)
        case Moving (hs0, _) =>
          Issuing (hs0, hs1)
        case Settled (hs0) if hs0 == hs1 =>
          Settled (hs0)
        case Settled (hs0) =>
          Issuing (hs0, hs1)
      }}}

  def forOneHost [H <: Host] (
      seed: Long
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Int =

    s"forOneHost (${seed}L, $config)"
    .withStubScheduler (seed, init) { kit =>
      import kit._

      val hs @ Seq (h1) = Seq (runner .install (H1) .pass)
      h1.setAtlas (settled (h1))

      network.messageFlakiness = config.messageFlakiness
      val cb = runner.setup (scheduler) (h1, h1) .capture()
      val count = scheduler.run (timers = !cb.wasInvoked)
      cb.passedOrTimedout
      network.messageFlakiness = 0.0

      runner.recover (scheduler) (h1) .pass
      runner.verify (hs)

      count
    }

  def forOneHost [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val average = forSeeds (forOneHost (_) (init))
    info (s"Average time on one host: ${average}ms")
  }

  def forThreeStableHosts [H <: Host] (
      seed: Long
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Int =

    s"forThreeStableHosts (${seed}L, $config)"
    .withStubScheduler (seed, init) { kit =>
      import kit._

      val h1 = runner .install (H1) .pass
      val h2 = runner .install (H2) .pass
      val h3 = runner .install (H3) .pass
      for (h <- Seq (h1, h2, h3))
        h.setAtlas (settled (h1, h2, h3))

      network.messageFlakiness = config.messageFlakiness
      val cb = runner.setup (scheduler) (h1, h2) .capture()
      val count = scheduler.run (timers = !cb.wasInvoked)
      cb.passedOrTimedout
      network.messageFlakiness = 0.0

      runner.recover (scheduler) (h1) .pass

      count
    }

  def forThreeStableHosts [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val average = forSeeds (forThreeStableHosts (_) (init))
    info (s"Average time on three stable hosts: ${average}ms")
  }

  def forThreeStableHostsMultithreaded [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Int =

    s"forThreeStableHostsMultithreaded ($config)"
    .withMultithreadedScheduler (init) { kit =>
      import kit._

      val h1 = runner .install (H1) .await()
      val h2 = runner .install (H2) .await()
      val h3 = runner .install (H3) .await()
      for (h <- Seq (h1, h2, h3))
        h.setAtlas (settled (h1, h2, h3))

      network.messageFlakiness = config.messageFlakiness
      val start = System.currentTimeMillis
      runner.setup (scheduler) (h1, h2) .passOrTimeout
      val end = System.currentTimeMillis
      network.messageFlakiness = 0.0

      runner.recover (scheduler) (h1) .await()

      (end - start).toInt
    }

  def forOneHostOffline [H <: Host] (
      seed: Long
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Int =

    s"forOneHostOffline (${seed}L, $config)"
    .withStubScheduler (seed, init) { kit =>
      import kit._

      val d1 = new StubDiskDrive
      val d2 = new StubDiskDrive

      var h1 = runner .boot (H1, d1, true) .pass
      var h2 = runner .boot (H2, d2, true) .pass
      val h3 = runner .install (H3) .pass
      h3.shutdown()
      for (h <- Seq (h1, h2, h3))
        h.setAtlas (settled (h1, h2, h3))

      network.messageFlakiness = config.messageFlakiness
      val cb = runner.setup (scheduler) (h1, h2) .capture()
      val count = scheduler.run (timers = !cb.wasInvoked, oblivious = true)
      cb.passedOrTimedout
      network.messageFlakiness = 0.0

      h1.shutdown()
      h2.shutdown()
      h1 = runner .boot (H1, d1, false) .pass
      h2 = runner .boot (H2, d2, false) .pass
      for (h <- Seq (h1, h2))
        h.setAtlas (settled (h1, h2, h3))
      val cb2 = runner.recover (scheduler) (h1) .capture()
      scheduler.run (timers = !cb2.wasInvoked, oblivious = true)
      cb2.passed

      count
    }

  def forOneHostOffline [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val average = forSeeds (forOneHostOffline (_) (init))
    info (s"Average time with one host crashed: ${average}ms")
  }

  def forOneHostOfflineMultithreaded [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Int =

    s"forOneHostOfflineMultithreaded ($config)"
    .withMultithreadedScheduler (init) { kit =>
      import kit._

      val d1 = new StubDiskDrive
      val d2 = new StubDiskDrive

      var h1 = runner .boot (H1, d1, true) .await()
      var h2 = runner .boot (H2, d2, true) .await()
      val h3 = runner .install (H3) .await()
      h3.shutdown()
      for (h <- Seq (h1, h2, h3))
        h.setAtlas (settled (h1, h2, h3))

      network.messageFlakiness = config.messageFlakiness
      val start = System.currentTimeMillis
      runner.setup (scheduler) (h1, h2) .passOrTimeout
      val end = System.currentTimeMillis
      network.messageFlakiness = 0.0

      h1.shutdown()
      h2.shutdown()
      h1 = runner .boot (H1, d1, false) .await()
      h2 = runner .boot (H2, d2, false) .await()
      for (h <- Seq (h1, h2))
        h.setAtlas (settled (h1, h2, h3))
      runner.recover (scheduler) (h1) .await()

      (end - start).toInt
    }

  def forOneHostCrashing [H <: Host] (
      seed: Long,
      target: Int
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Int =

    s"forOneHostCrashing (${seed}L, $target, $config)"
    .withStubScheduler (seed, init) { kit =>
      import kit._

      val d1 = new StubDiskDrive
      val d2 = new StubDiskDrive
      val d3 = new StubDiskDrive

      var h1 = runner .boot (H1, d1, true) .pass
      var h2 = runner .boot (H2, d2, true) .pass
      var h3 = runner .boot (H3, d3, true) .pass
      for (h <- Seq (h1, h2, h3))
        h.setAtlas (settled (h1, h2, h3))

      network.messageFlakiness = config.messageFlakiness
      val cb = runner.setup (scheduler) (h1, h2) .capture()
      scheduler.run (count = target, timers = true)
      h3.shutdown()
      val count = scheduler.run (timers = !cb.wasInvoked, oblivious = true)
      cb.passedOrTimedout
      network.messageFlakiness = 0.0

      h1.shutdown()
      h2.shutdown()
      h1 = runner .boot (H1, d1, false) .pass
      h2 = runner .boot (H2, d2, false) .pass
      h3 = runner .boot (H3, d3, false) .pass
      for (h <- Seq (h1, h2, h3))
        h.setAtlas (settled (h1, h2, h3))
      runner .recover (scheduler) (h1) .pass

      count
    }

  def forOneHostCrashing [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val average = forTargets (
        forThreeStableHosts (_) (init)) (
            forOneHostCrashing (_, _) (init))
    info (s"Average time with one host crashing: ${average}ms")
  }

  def forOneHostCrashingMultithreaded [H <: Host] (
      target: Int
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Int =

    s"forOneHostCrashingMultithreaded ($target, $config)"
    .withMultithreadedScheduler (init) { kit =>
      import kit._

      val d1 = new StubDiskDrive
      val d2 = new StubDiskDrive
      val d3 = new StubDiskDrive

      var h1 = runner .boot (H1, d1, true) .await()
      var h2 = runner .boot (H2, d2, true) .await()
      var h3 = runner .boot (H3, d3, true) .await()
      for (h <- Seq (h1, h2, h3))
        h.setAtlas (settled (h1, h2, h3))

      network.messageFlakiness = config.messageFlakiness
      scheduler.delay (target) (h3.shutdown())
      val start = System.currentTimeMillis
      runner.setup (scheduler) (h1, h2) .passOrTimeout
      val end = System.currentTimeMillis
      network.messageFlakiness = 0.0

      h1.shutdown()
      h2.shutdown()
      h3.shutdown()
      h1 = runner .boot (H1, d1, false) .await()
      h2 = runner .boot (H2, d2, false) .await()
      h3 = runner .boot (H3, d3, false) .await()
      for (h <- Seq (h1, h2, h3))
        h.setAtlas (settled (h1, h2, h3))

      runner .recover (scheduler) (h1) .await()

      (end - start).toInt
    }

  def forOneHostCrashingMultithreaded [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val time = forThreeStableHostsMultithreaded (init)
    val target =  Random.nextInt ((time * 0.7).toInt) + (time * 0.1).toInt
    forOneHostCrashingMultithreaded (target) (init)
  }

  def forOneHostRebooting [H <: Host] (
      seed: Long,
      target: Int
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Unit =

    s"forOneHostRebooting (${seed}L, $target, $config)"
    .withStubScheduler (seed, init) { kit =>
      import kit._

      val d1 = new StubDiskDrive
      val d2 = new StubDiskDrive
      val d3 = new StubDiskDrive

      var h1 = runner .boot (H1, d1, true) .pass
      var h2 = runner .boot (H2, d2, true) .pass
      var h3 = runner .boot (H3, d3, true) .pass
      h3.shutdown()
      for (h <- Seq (h1, h2, h3))
        h.setAtlas (settled (h1, h2, h3))

      network.messageFlakiness = config.messageFlakiness
      val cb = runner.setup (scheduler) (h1, h2) .capture()
      scheduler.run (count = target, timers = true, oblivious = true)
      val cb2 = runner .boot (H3, d3, false) .capture()
      scheduler.run (timers = !cb.wasInvoked || !cb2.wasInvoked, oblivious = true)
      cb.passedOrTimedout
      h3 = cb2.passed
      network.messageFlakiness = 0.0

      h1.shutdown()
      h2.shutdown()
      h3.shutdown()
      h1 = runner .boot (H1, d1, false) .pass
      h2 = runner .boot (H2, d2, false) .pass
      h3 = runner .boot (H3, d3, false) .pass
      for (h <- Seq (h1, h2, h3))
        h.setAtlas (settled (h1, h2, h3))
      runner .recover (scheduler) (h1) .pass
    }

  def forOneHostRebooting [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val average = forTargets (
        forOneHostOffline (_) (init)) (
            forOneHostRebooting (_, _) (init))
    info (s"Average time with one host rebooting: ${average}ms")
  }

  def forOneHostRebootingMultithreaded [H <: Host] (
      target: Int
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Unit =

    s"forOneHostRebootingMultithreaded ($target, $config)"
    .withMultithreadedScheduler (init) { kit =>
      import kit._

      val d1 = new StubDiskDrive
      val d2 = new StubDiskDrive
      val d3 = new StubDiskDrive

      var h1 = runner .boot (H1, d1, true) .await()
      var h2 = runner .boot (H2, d2, true) .await()
      var h3 = runner .boot (H3, d3, true) .await()
      h3.shutdown()
      for (h <- Seq (h1, h2, h3))
        h.setAtlas (settled (h1, h2, h3))

      network.messageFlakiness = config.messageFlakiness
      val _h3 =
        (for {
          _ <- Async.delay (target)
          h <- runner.boot (H3, d3, false)
        } yield h) .toFuture
      runner.setup (scheduler) (h1, h2) .passOrTimeout
      h3 = _h3.await()
      network.messageFlakiness = 0.0

      h1.shutdown()
      h2.shutdown()
      h3.shutdown()
      h1 = runner .boot (H1, d1, false) .await()
      h2 = runner .boot (H2, d2, false) .await()
      h3 = runner .boot (H3, d3, false) .await()
      for (h <- Seq (h1, h2, h3))
        h.setAtlas (settled (h1, h2, h3))
      runner .recover (scheduler) (h1) .await()
    }

  def forOneHostRebootingMultithreaded [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val time = forOneHostOfflineMultithreaded (init)
    val target =  Random.nextInt ((time * 0.7).toInt) + (time * 0.1).toInt
    forOneHostRebootingMultithreaded (target) (init)
  }

  def forOneHostBouncing [H <: Host] (
      seed: Long,
      target1: Int,
      target2: Int
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Unit =

    s"forOneHostBouncing (${seed}L, $target1, $target2, $config)"
    .withStubScheduler (seed, init) { kit =>
      import kit._

      val d1 = new StubDiskDrive
      val d2 = new StubDiskDrive
      val d3 = new StubDiskDrive

      var h1 = runner .boot (H1, d1, true) .pass
      var h2 = runner .boot (H2, d2, true) .pass
      var h3 = runner .boot (H3, d3, true) .pass
      for (h <- Seq (h1, h2, h3))
        h.setAtlas (settled (h1, h2, h3))

      network.messageFlakiness = config.messageFlakiness
      val cb = runner.setup (scheduler) (h1, h2) .capture()
      scheduler.run (count = target1, timers = true, oblivious = true)
      h3.shutdown()
      scheduler.run (count = target2, timers = true, oblivious = true)
      val cb2 = runner .boot (H3, d3, false) .capture()
      scheduler.run (timers = !cb.wasInvoked || !cb2.wasInvoked, oblivious = true)
      cb.passedOrTimedout
      h3 = cb2.passed
      network.messageFlakiness = 0.0

      h1.shutdown()
      h2.shutdown()
      h3.shutdown()
      h1 = runner .boot (H1, d1, false) .pass
      h2 = runner .boot (H2, d2, false) .pass
      h3 = runner .boot (H3, d3, false) .pass
      for (h <- Seq (h1, h2, h3))
        h.setAtlas (settled (h1, h2, h3))
      runner .recover (scheduler) (h1) .pass
    }

  def forOneHostBouncing [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val average = forDoubleTargets (
        forThreeStableHosts (_) (init)) (
            forOneHostCrashing (_, _) (init)) (
                forOneHostBouncing (_, _, _) (init))
    info (s"Average time with one host bouncing: ${average}ms")
  }

  def forOneHostBouncingMultithreaded [H <: Host] (
      target1: Int,
      target2: Int
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Unit =

    s"forOneHostRebootingMultithreaded ($target1, $target2, $config)"
    .withMultithreadedScheduler (init) { kit =>
      import kit._

      val d1 = new StubDiskDrive
      val d2 = new StubDiskDrive
      val d3 = new StubDiskDrive

      var h1 = runner .boot (H1, d1, true) .await()
      var h2 = runner .boot (H2, d2, true) .await()
      var h3 = runner .boot (H3, d3, true) .await()
      for (h <- Seq (h1, h2, h3))
        h.setAtlas (settled (h1, h2, h3))

      network.messageFlakiness = config.messageFlakiness
      val _h3 =
        (for {
          _ <- Async.delay (target1)
          _ = h3.shutdown()
          _ <- Async.delay (target2)
          h <- runner .boot (H3, d3, false)
        } yield h) .toFuture
      runner.setup (scheduler) (h1, h2) .passOrTimeout
      h3 = _h3.await()
      network.messageFlakiness = 0.0

      h1.shutdown()
      h2.shutdown()
      h3.shutdown()
      h1 = runner .boot (H1, d1, false) .await()
      h2 = runner .boot (H2, d2, false) .await()
      h3 = runner .boot (H3, d3, false) .await()
      for (h <- Seq (h1, h2, h3))
        h.setAtlas (settled (h1, h2, h3))
      runner .recover (scheduler) (h1) .await()
    }

  def forOneHostBouncingMultithreaded [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val time1 = forThreeStableHostsMultithreaded (init)
    val target1 = Random.nextInt ((time1 * 0.7).toInt) + (time1 * 0.1).toInt
    val time2 = forOneHostCrashingMultithreaded (target1) (init)
    val target2 =  Random.nextInt ((time2 * 0.7).toInt) + (time2 * 0.1).toInt
    forOneHostBouncingMultithreaded (target1, target2) (init)
  }

  def forOneHostMoving [H <: Host] (
      seed: Long,
      target: Int
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Unit =

    s"forOneHostMoving (${seed}L, $target, $config)"
    .withStubScheduler (seed, init) { kit =>
      import kit._

      val h1 = runner .install (H1) .pass
      val h2 = runner .install (H2) .pass
      val hs = Seq (h1, h2)
      for (h1 <- hs; h2 <- hs)
        h1.hail (h2.localId)
      h1.issueAtlas (settled (h1)) .pass

      network.messageFlakiness = config.messageFlakiness
      val cb = runner.setup (scheduler) (h1, h1) .capture()
      scheduler.run (count = target, timers = true)
      h1.issueAtlas (issuing (h1) (h2)) .pass
      scheduler.run (timers = !cb.wasInvoked)
      cb.passedOrTimedout
      network.messageFlakiness = 0.0

      runner.recover (scheduler) (h1) .pass
      runner.verify (hs)
    }

  def forOneHostMoving [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val average = forTargets (
        forOneHost (_) (init)) (
            forOneHostMoving (_, _) (init))
    info (s"Average time with one host moving: ${average}ms")
  }

  def forThreeHostsMoving [H <: Host] (
      seed: Long,
      target: Int
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Unit =

    s"forThreeHostsMoving (${seed}L, $target, $config)"
    .withStubScheduler (seed, init) { kit =>
      import kit._

      val hs @ Seq (h1, h2, h3, h4, h5, h6) =
        for (id <- Seq (H1, H2, H3, H4, H5, H6))
          yield runner .install (id) .pass
      for (h1 <- hs; h2 <- hs)
        h1.hail (h2.localId)
      h1.issueAtlas (settled (h1, h2, h3)) .pass

      network.messageFlakiness = config.messageFlakiness
      val cb = runner.setup (scheduler) (h1, h2) .capture()
      scheduler.run (count = target, timers = true)
      h1.issueAtlas (issuing (h1, h2, h3) (h4, h5, h6)) .pass
      scheduler.run (timers = !cb.wasInvoked)
      cb.passedOrTimedout
      network.messageFlakiness = 0.0

      runner.recover (scheduler) (h1) .pass
      runner.verify (hs)
    }

    def forThreeHostsMoving [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val average = forTargets (
        forThreeStableHosts (_) (init)) (
            forThreeHostsMoving (_, _) (init))
    info (s"Average time with three hosts moving: ${average}ms")
  }

  def for3to8 [H <: Host] (
      seed: Long,
      target: Int
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Unit =

    s"for3to8 (${seed}L, $target, $config)"
    .withStubScheduler (seed, init) { kit =>
      import kit._

      val hs =
        for (id <- Seq (H1, H2, H3, H4, H5, H6, H7, H8))
          yield runner .install (id) .pass
      val Seq (h1, h2, h3) = hs take 3
      for (h1 <- hs; h2 <- hs)
        h1.hail (h2.localId)
      val atlas1 = Seq (settled (h1, h2, h3))
      h1.issueAtlas (atlas1: _*) .pass

      network.messageFlakiness = config.messageFlakiness
      val cb = runner.setup (scheduler) (h1, h2) .capture()
      scheduler.run (count = target, timers = true)
      h1.issueAtlas (rewriteFor8 (atlas1, hs): _*) .pass
      scheduler.run (timers = !cb.wasInvoked)
      cb.passedOrTimedout
      network.messageFlakiness = 0.0

      runner.recover (scheduler) (h1) .pass
      runner.verify (hs)
    }

    def for3to8 [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val average = forTargets (
        forThreeStableHosts (_) (init)) (
            for3to8 (_, _) (init))
    info (s"Average time for 3 growing to 8: ${average}ms")
  }}

object StoreClusterChecks {

  trait Host extends StubStoreHost {

    def setAtlas (cohorts: Cohort*)
    def issueAtlas (cohorts: Cohort*): Async [Unit]
    def shutdown()
  }

  trait Package [H] {

    def boot (
        id: HostId,
        drive: StubDiskDrive,
        init: Boolean
    ) (implicit
        random: Random,
        parent: Scheduler,
        network: StubNetwork,
        config: StoreTestConfig
    ): Async [H]
  }}
