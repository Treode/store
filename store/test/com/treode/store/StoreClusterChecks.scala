package com.treode.store

import java.util.concurrent.Executors
import scala.util.Random

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.async.implicits._
import com.treode.async.stubs.{AsyncChecks, CallbackCaptor, StubScheduler}
import com.treode.async.stubs.implicits._
import com.treode.cluster.HostId
import com.treode.cluster.stubs.StubNetwork
import com.treode.disk.stubs.StubDiskDrive
import org.scalatest.{Assertions, Informing, Suite}
import org.scalatest.time.SpanSugar

import Async.async
import Callback.{ignore => disregard}
import SpanSugar._
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

  override val timeLimit = 15 minutes

  val H1 = 0x44L
  val H2 = 0x75L
  val H3 = 0xC3L
  val H4 = 0x7EL
  val H5 = 0x8CL
  val H6 = 0x32L
  val H7 = 0xEDL
  val H8 = 0xBFL
  val HS = Seq (H1, H2, H3, H4, H5, H6, H7, H8)

  class ForStoreClusterRunner [H <: Host] (
      val messages: Seq [String],
      val pkg: Package [H],
      val _setup: Scheduler => (H, H) => Async [_],
      val _verifyCond: Seq [H] => Boolean,
      val _verify: Scheduler => H => Async [_],
      val _auditCond: Seq [H] => Boolean,
      val _audit: Scheduler => Seq [H] => Async [_]
  ) {

    def install (
        id: HostId
    ) (implicit
        random: Random,
        parent: Scheduler,
        network: StubNetwork,
        config: StoreTestConfig
    ): Async [H] =
      pkg.boot (id, new StubDiskDrive, true)

    def install (
        id: HostId,
        drive: StubDiskDrive
    ) (implicit
        random: Random,
        parent: Scheduler,
        network: StubNetwork,
        config: StoreTestConfig
    ): Async [H] =
      pkg.boot (id, drive, true)

    def reboot (
        id: HostId,
        drive: StubDiskDrive
    ) (implicit
        random: Random,
        parent: Scheduler,
        network: StubNetwork,
        config: StoreTestConfig
    ): Async [H] =
      pkg.boot (id, drive, false)

    def setup (
        h1: H,
        h2: H
    ) (implicit
        scheduler: StubScheduler,
        network: StubNetwork,
        config: StoreTestConfig
    ): Async [Unit] = {
      network.messageFlakiness = config.messageFlakiness
      for {
        _ <- _setup (scheduler) (h1, h2)
      } yield {
        network.messageFlakiness = 0.0
      }}

    def verify (
        hs: Seq [H]
    ) (implicit
        scheduler: StubScheduler,
        network: StubNetwork
    ): Async [Unit] = {
      scheduler.run (timers = _verifyCond (hs))
      for {
        _ <- _verify (scheduler) (hs.head)
        _ = scheduler.run (timers = _auditCond (hs))
        _ <- _audit (scheduler) (hs) .map (_ => ())
      } yield ()
    }}

  class ForStoreClusterAudit [H <: Host] (
      val messages: Seq [String],
      val pkg: Package [H],
      val setup: Scheduler => (H, H) => Async [_],
      val verifyCond: Seq [H] => Boolean,
      val verify: Scheduler => H => Async [_],
      val auditCond: Seq [H] => Boolean
  ) {

    def audit (test: Scheduler => Seq [H] => Async [Unit]) =
      new ForStoreClusterRunner [H] (messages, pkg, setup, verifyCond, verify, auditCond, test)
  }

  class ForStoreClusterVerifyCond [H <: Host] (
      val messages: Seq [String],
      val pkg: Package [H],
      val setup: Scheduler => (H, H) => Async [_],
      val verifyCond: Seq [H] => Boolean,
      val verify: Scheduler => H => Async [_]
  ) {

    def whilst (cond: Seq [H] => Boolean) =
      new ForStoreClusterAudit [H] (messages, pkg, setup, verifyCond, verify, cond)

    def audit (test: Scheduler => Seq [H] => Async [Unit])  =
      new ForStoreClusterRunner [H] (messages, pkg, setup, verifyCond, verify, _ => false, test)
  }

  class ForStoreClusterRecover [H <: Host] (
      messages: Seq [String],
      pkg: Package [H],
      setup: Scheduler => (H, H) => Async [_],
      verifyCond: Seq [H] => Boolean
  ) {

    def verify (test: Scheduler => H => Async [_]) =
      new ForStoreClusterVerifyCond [H] (messages, pkg, setup, verifyCond, test)
  }

  class ForStoreClusterSetupCond [H <: Host] (
      messages: Seq [String],
      pkg: Package [H],
      setup: Scheduler => (H, H) => Async [_]
  ) {

    def whilst (cond: Seq [H] => Boolean) =
      new ForStoreClusterRecover [H] (messages, pkg, setup, cond)

    def verify (test: Scheduler => H => Async [_]) =
      new ForStoreClusterVerifyCond [H] (messages, pkg, setup, _ => false, test)
  }

  class ForStoreClusterSetup [H <: Host] (
      messages: Seq [String],
      pkg: Package [H]
  ) {

    def setup (setup: Scheduler => (H, H) => Async [_]) =
      new ForStoreClusterSetupCond (messages, pkg, setup)
  }

  class ForStoreClusterHost {

    private val messages = Seq.newBuilder [String]

    def info (msg: String): ForStoreClusterHost = {
      messages += msg
      this
    }

    def host [H <: Host] (pkg: Package [H]): ForStoreClusterSetup [H] =
      new ForStoreClusterSetup (messages.result, pkg)
  }

  def cluster: ForStoreClusterHost =
    new ForStoreClusterHost

  private class StubSchedulerKit [H <: Host] (
      val runner: ForStoreClusterRunner [H]
   ) (implicit
      val random: Random,
      val scheduler: StubScheduler,
      val network: StubNetwork
  )

  private implicit class NamedTest (name: String) {

    def withRandomScheduler [H <: Host, A] (
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

    def withMultithreadedScheduler [H <: Host, A] (
        init: Random => ForStoreClusterRunner [H]
    ) (
        test: StubSchedulerKit [H] => A
    ): A = {
      implicit val random = Random
      val executor = Executors.newScheduledThreadPool (nthreads)
      val runner = init (random)
      try {
        implicit val scheduler = StubScheduler.multithreaded (executor)
        implicit val network = StubNetwork (random)
        test (new StubSchedulerKit (runner))
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

  private def rewriteFor3to8 (prev: Seq [Cohort], hosts: Seq [StubStoreHost]): Seq [Cohort] = {
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

  private def rewriteFor8to3 (prev: Seq [Cohort], hosts: Seq [StubStoreHost]): Seq [Cohort] = {
    import Cohort.{Issuing, Moving, Settled}
    val hs1 = hosts .take (3) .map (_.localId) .toSet
    for ((Settled (hs0), i) <- prev.zipWithIndex) yield
      if (hs0 == hs1)
        Settled (hs0)
      else
        Issuing (hs0, hs1)
  }

  def for1host [H <: Host] (
      seed: Long
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Int =

    s"for1host (${seed}L, $config)"
    .withRandomScheduler (seed, init) { kit =>
      import kit._

      val h1 = runner.install (H1) .pass
      val hs = Seq (h1)
      h1.setAtlas (settled (h1))

      val cb = runner.setup  (h1, h1) .capture()
      val count = scheduler.run (timers = !cb.wasInvoked)
      cb.passedOrTimedout

      runner.verify (hs) .pass

      count
    }

  def for1host [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val average = forSeeds (for1host (_) (init))
    info (s"Average time on one host: ${average}ms")
  }

  def for3hosts [H <: Host] (
      seed: Long
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Int =

    s"for3hosts (${seed}L, $config)"
    .withRandomScheduler (seed, init) { kit =>
      import kit._

      val h1 = runner.install (H1) .pass
      val h2 = runner.install (H2) .pass
      val h3 = runner.install (H3) .pass
      val hs = Seq (h1, h2, h3)
      for (h <- hs)
        h.setAtlas (settled (h1, h2, h3))

      val cb = runner.setup (h1, h2) .capture()
      val count = scheduler.run (timers = !cb.wasInvoked)
      cb.passedOrTimedout

      runner.verify (hs) .pass

      count
    }

  def for3hosts [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val average = forSeeds (for3hosts (_) (init))
    info (s"Average time on three stable hosts: ${average}ms")
  }

  def for3hostsMT [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Int =

    s"for3hostsMT ($config)"
    .withMultithreadedScheduler (init) { kit =>
      import kit._

      val h1 = runner.install (H1) .await()
      val h2 = runner.install (H2) .await()
      val h3 = runner.install (H3) .await()
      val hs = Seq (h1, h2, h3)
      for (h <- hs)
        h.setAtlas (settled (h1, h2, h3))

      val start = System.currentTimeMillis
      runner.setup (h1, h2) .passOrTimeout
      val end = System.currentTimeMillis

      runner.verify (hs) .await()

      (end - start).toInt
    }

  def for8hosts [H <: Host] (
      seed: Long
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Int =

    s"for8hosts (${seed}L, $config)"
    .withRandomScheduler (seed, init) { kit =>
      import kit._

      val hs =
        for (id <- Seq (H1, H2, H3, H4, H5, H6, H7, H8))
          yield runner.install (id) .pass
      val Seq (h1, h2) = hs take 2
      for (h1 <- hs; h2 <- hs)
        h1.hail (h2.localId)
      h1.issueAtlas (cohortsFor8 (hs): _*) .pass

      val cb = runner.setup (h1, h2) .capture()
      val count = scheduler.run (timers = !cb.wasInvoked)
      cb.passedOrTimedout

      runner.verify (hs) .pass

      count
    }

  def for8hosts [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val average = forSeeds (for8hosts (_) (init))
    info (s"Average time on eight stable hosts: ${average}ms")
  }

  def for3with1offline [H <: Host] (
      seed: Long
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Int =

    s"for3with1offline (${seed}L, $config)"
    .withRandomScheduler (seed, init) { kit =>
      import kit._

      val d1 = new StubDiskDrive
      val d2 = new StubDiskDrive

      val h1 = runner.install (H1, d1) .pass
      val h2 = runner.install (H2, d2) .pass
      val h3 = runner.install (H3) .pass
      val hs = Seq (h1, h2)
      h3.shutdown()
      for (h <- hs)
        h.setAtlas (settled (h1, h2, h3))

      val cb = runner.setup (h1, h2) .capture()
      val count = scheduler.run (timers = !cb.wasInvoked, oblivious = true)
      cb.passedOrTimedout

      for (h <- hs)
        h.setAtlas (settled (h1, h2, h3))
      runner.verify (hs) .pass

      count
    }

  def for3with1offline [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val average = forSeeds (for3with1offline (_) (init))
    info (s"Average time with one host crashed: ${average}ms")
  }

  def for3with1offlineMT [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Int =

    s"for3with1offlineMT ($config)"
    .withMultithreadedScheduler (init) { kit =>
      import kit._

      val d1 = new StubDiskDrive
      val d2 = new StubDiskDrive

      val h1 = runner.install (H1, d1) .await()
      val h2 = runner.install (H2, d2) .await()
      val h3 = runner.install (H3) .await()
      h3.shutdown()
      val hs = Seq (h1, h2)
      for (h <- hs)
        h.setAtlas (settled (h1, h2, h3))

      val start = System.currentTimeMillis
      runner.setup (h1, h2) .passOrTimeout
      val end = System.currentTimeMillis

      for (h <- hs)
        h.setAtlas (settled (h1, h2, h3))
      runner.verify (hs) .await()

      (end - start).toInt
    }

  def for3with1crashing [H <: Host] (
      seed: Long,
      target: Int
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Int =

    s"for3with1crashing (${seed}L, $target, $config)"
    .withRandomScheduler (seed, init) { kit =>
      import kit._

      val d3 = new StubDiskDrive

      val h1 = runner.install (H1) .pass
      val h2 = runner.install (H2) .pass
      var h3 = runner.install (H3, d3) .pass
      var hs = Seq (h1, h2, h3)
      for (h <- hs)
        h.setAtlas (settled (h1, h2, h3))

      val cb = runner.setup (h1, h2) .capture()
      scheduler.run (count = target, timers = true)
      h3.shutdown()
      val count = scheduler.run (timers = !cb.wasInvoked, oblivious = true)
      cb.passedOrTimedout

      h3 = runner.reboot (H3, d3) .pass
      hs = Seq (h1, h2, h3)
      for (h <- hs)
        h.setAtlas (settled (h1, h2, h3))
      runner.verify (hs) .pass

      count
    }

  def for3with1crashing [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val average = forTargets (
        for3hosts (_) (init)) (
            for3with1crashing (_, _) (init))
    info (s"Average time with one host crashing: ${average}ms")
  }

  def for3with1crashingMT [H <: Host] (
      target: Int
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Int =

    s"for3with1crashingMT ($target, $config)"
    .withMultithreadedScheduler (init) { kit =>
      import kit._

      val d3 = new StubDiskDrive

      val h1 = runner.install (H1) .await()
      val h2 = runner.install (H2) .await()
      var h3 = runner.install (H3, d3) .await()
      var hs = Seq (h1, h2, h3)
      for (h <- hs)
        h.setAtlas (settled (h1, h2, h3))

      val start = System.currentTimeMillis
      scheduler.delay (target) (h3.shutdown())
      runner.setup (h1, h2) .passOrTimeout
      val end = System.currentTimeMillis

      h3 = runner.reboot (H3, d3) .await()
      hs = Seq (h1, h2, h3)
      for (h <- hs)
        h.setAtlas (settled (h1, h2, h3))

      runner.verify (hs) .await()

      (end - start).toInt
    }

  def for3with1crashingMT [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val time = for3hostsMT (init)
    val target =  Random.nextInt ((time * 0.7).toInt) + (time * 0.1).toInt
    for3with1crashingMT (target) (init)
  }

  def for3with1rebooting [H <: Host] (
      seed: Long,
      target: Int
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Unit =

    s"for3with1rebooting (${seed}L, $target, $config)"
    .withRandomScheduler (seed, init) { kit =>
      import kit._

      val d3 = new StubDiskDrive

      val h1 = runner.install (H1) .pass
      val h2 = runner.install (H2) .pass
      var h3 = runner.install (H3, d3) .pass
      var hs = Seq (h1, h2, h3)
      h3.shutdown()
      for (h <- hs)
        h.setAtlas (settled (h1, h2, h3))

      val cb = runner.setup (h1, h2) .capture()
      scheduler.run (count = target, timers = true, oblivious = true)
      val cb2 = runner.reboot (H3, d3) .capture()
      scheduler.run (timers = !cb.wasInvoked || !cb2.wasInvoked, oblivious = true)
      cb.passedOrTimedout
      h3 = cb2.passed

      hs = Seq (h1, h2, h3)
      for (h <- hs)
        h.setAtlas (settled (h1, h2, h3))
      runner.verify (hs) .pass
    }

  def for3with1rebooting [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val average = forTargets (
        for3with1offline (_) (init)) (
            for3with1rebooting (_, _) (init))
    info (s"Average time with one host rebooting: ${average}ms")
  }

  def for3with1bouncing [H <: Host] (
      seed: Long,
      target1: Int,
      target2: Int
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Unit =

    s"for3with1bouncing (${seed}L, $target1, $target2, $config)"
    .withRandomScheduler (seed, init) { kit =>
      import kit._

      val d3 = new StubDiskDrive

      val h1 = runner.install (H1) .pass
      val h2 = runner.install (H2) .pass
      var h3 = runner.install (H3, d3) .pass
      var hs = Seq (h1, h2, h3)
      for (h <- hs)
        h.setAtlas (settled (h1, h2, h3))

      val cb = runner.setup (h1, h2) .capture()
      scheduler.run (count = target1, timers = true, oblivious = true)
      h3.shutdown()
      scheduler.run (count = target2, timers = true, oblivious = true)
      val cb2 = runner.reboot (H3, d3) .capture()
      scheduler.run (timers = !cb.wasInvoked || !cb2.wasInvoked, oblivious = true)
      cb.passedOrTimedout
      h3 = cb2.passed

      hs = Seq (h1, h2, h3)
      for (h <- hs)
        h.setAtlas (settled (h1, h2, h3))
      runner.verify (hs) .pass
    }

  def for3with1bouncing [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val average = forDoubleTargets (
        for3hosts (_) (init)) (
            for3with1crashing (_, _) (init)) (
                for3with1bouncing (_, _, _) (init))
    info (s"Average time with one host bouncing: ${average}ms")
  }

  def for3with1bouncingMT [H <: Host] (
      target1: Int,
      target2: Int
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Unit =

    s"for3with1rebootingMT ($target1, $target2, $config)"
    .withMultithreadedScheduler (init) { kit =>
      import kit._

      val d3 = new StubDiskDrive

      val h1 = runner.install (H1) .await()
      val h2 = runner.install (H2) .await()
      var h3 = runner.install (H3, d3) .await()
      var hs = Seq (h1, h2, h3)
      for (h <- hs)
        h.setAtlas (settled (h1, h2, h3))

      val _h3 =
        (for {
          _ <- Async.delay (target1)
          _ = h3.shutdown()
          _ <- Async.delay (target2)
          h <- runner.reboot (H3, d3)
        } yield h) .toFuture
      runner.setup (h1, h2) .passOrTimeout
      h3 = _h3.await()

      hs = Seq (h1, h2, h3)
      for (h <- hs)
        h.setAtlas (settled (h1, h2, h3))
      runner.verify (hs) .await()
    }

  def for3with1bouncingMT [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val time1 = for3hostsMT (init)
    val target1 = Random.nextInt ((time1 * 0.7).toInt) + (time1 * 0.1).toInt
    val time2 = for3with1crashingMT (target1) (init)
    val target2 =  Random.nextInt ((time2 * 0.7).toInt) + (time2 * 0.1).toInt
    for3with1bouncingMT (target1, target2) (init)
  }

  def for1to1 [H <: Host] (
      seed: Long,
      target: Int
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Unit =

    s"for1to1 (${seed}L, $target, $config)"
    .withRandomScheduler (seed, init) { kit =>
      import kit._

      val h1 = runner.install (H1) .pass
      val h2 = runner.install (H2) .pass
      val hs = Seq (h1, h2)
      for (h1 <- hs; h2 <- hs)
        h1.hail (h2.localId)
      h1.issueAtlas (settled (h1)) .pass

      val cb = runner.setup (h1, h1) .capture()
      scheduler.run (count = target, timers = true)
      h1.issueAtlas (issuing (h1) (h2)) .pass
      scheduler.run (timers = !cb.wasInvoked)
      cb.passedOrTimedout

      runner.verify (hs) .pass
    }

  def for1to1 [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val average = forTargets (
        for1host (_) (init)) (
            for1to1 (_, _) (init))
    info (s"Average time with one host moving: ${average}ms")
  }

  def for1to3 [H <: Host] (
      seed: Long,
      target: Int
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Int =

    s"for1to3 (${seed}L, $target, $config)"
    .withRandomScheduler (seed, init) { kit =>
      import kit._

      val h1 = runner.install (H1) .pass
      val h2 = runner.install (H2) .pass
      val h3 = runner.install (H3) .pass
      val hs = Seq (h1, h2, h3)
      for (h1 <- hs; h2 <- hs)
        h1.hail (h2.localId)
      h1.issueAtlas (settled (h1)) .pass

      val cb = runner.setup (h1, h2) .capture()
      scheduler.run (count = target, timers = true)
      h1.issueAtlas (issuing (h1) (h1, h2, h3)) .pass
      val count = scheduler.run (timers = !cb.wasInvoked)
      cb.passedOrTimedout

      runner.verify (hs) .pass

      count
    }

  def for1to3 [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val average = forTargets (
        for1host (_) (init)) (
            for1to3 (_, _) (init))
    info (s"Average time with one host growing to three: ${average}ms")
  }

  def for1to3with1bouncing [H <: Host] (
      seed: Long,
      target1: Int,
      target3: Int
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Unit =

    s"for1to3with1bouncing (${seed}L, $target1, $target3, $config)"
    .withRandomScheduler (seed, init) { kit =>
      import kit._

      val target2 = random.nextInt (target3)
      val d3 = new StubDiskDrive

      val h1 = runner.install (H1) .pass
      val h2 = runner.install (H2) .pass
      var h3 = runner.install (H3, d3) .pass
      var hs = Seq (h1, h2, h3)
      for (h1 <- hs; h2 <- hs)
        h1.hail (h2.localId)
      h1.issueAtlas (settled (h1)) .pass

      val cb = runner.setup (h1, h2) .capture()
      scheduler.run (count = target1, timers = true)
      h1.issueAtlas (issuing (h1) (h1, h2, h3)) .pass
      scheduler.run (count = target2, timers = true)
      h3.shutdown()
      scheduler.run (count = target3 - target2, timers = true)
      val cb2 = runner.reboot (H3, d3) .capture()
      scheduler.run (timers = !cb.wasInvoked || !cb2.wasInvoked)
      cb.passedOrTimedout
      h3 = cb2.passed

      hs = Seq (h1, h2, h3)
      runner.verify (hs) .pass
    }

  def for1to3with1bouncing [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val average = forDoubleTargets (
        for1host (_) (init)) (
            for1to3 (_, _) (init)) (
                for1to3with1bouncing (_, _, _) (init))
    info (s"Average time with one host growing to three, one bounces: ${average}ms")
  }

  def for3to1 [H <: Host] (
      seed: Long,
      target: Int
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Int =

    s"for3to1 (${seed}L, $target, $config)"
    .withRandomScheduler (seed, init) { kit =>
      import kit._

      val h1 = runner.install (H1) .pass
      val h2 = runner.install (H2) .pass
      val h3 = runner.install (H3) .pass
      val hs = Seq (h1, h2, h3)
      for (h1 <- hs; h2 <- hs)
        h1.hail (h2.localId)
      h1.issueAtlas (settled (h1, h2, h3)) .pass

      val cb = runner.setup (h1, h2) .capture()
      scheduler.run (count = target, timers = true)
      h1.issueAtlas (issuing (h1, h2, h3) (h1)) .pass
      val count = scheduler.run (timers = !cb.wasInvoked)
      cb.passedOrTimedout

      runner.verify (hs) .pass

      count
    }

  def for3to1 [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val average = forTargets (
        for1host (_) (init)) (
            for3to1 (_, _) (init))
    info (s"Average time with three hosts shriking to one: ${average}ms")
  }

  def for3to1with1bouncing [H <: Host] (
      seed: Long,
      target1: Int,
      target3: Int
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Unit =

    s"for3to1with1bouncing (${seed}L, $target1, $target3, $config)"
    .withRandomScheduler (seed, init) { kit =>
      import kit._

      val target2 = random.nextInt (target3)
      val d3 = new StubDiskDrive

      val h1 = runner.install (H1) .pass
      val h2 = runner.install (H2) .pass
      var h3 = runner.install (H3, d3) .pass
      var hs = Seq (h1, h2, h3)
      for (h1 <- hs; h2 <- hs)
        h1.hail (h2.localId)
      h1.issueAtlas (settled (h1, h2, h3)) .pass

      val cb = runner.setup (h1, h2) .capture()
      scheduler.run (count = target1, timers = true)
      h1.issueAtlas (issuing (h1, h2, h3) (h1)) .pass
      scheduler.run (count = target2, timers = true)
      h3.shutdown()
      scheduler.run (count = target3 - target2, timers = true)
      val cb2 = runner.reboot (H3, d3) .capture()
      scheduler.run (timers = !cb.wasInvoked || !cb2.wasInvoked)
      cb.passedOrTimedout
      h3 = cb2.passed

      hs = Seq (h1, h2, h3)
      runner.verify (hs) .pass
    }

  def for3to1with1bouncing [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val average = forDoubleTargets (
        for3hosts (_) (init)) (
            for3to1 (_, _) (init)) (
                for3to1with1bouncing (_, _, _) (init))
    info (s"Average time with three hosts shriking to one, one bounces: ${average}ms")
  }

  def for3replacing1 [H <: Host] (
      seed: Long,
      target: Int
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Int =

    s"for3replacing1 (${seed}L, $target, $config)"
    .withRandomScheduler (seed, init) { kit =>
      import kit._

      val h1 = runner.install (H1) .pass
      val h2 = runner.install (H2) .pass
      val h3 = runner.install (H3) .pass
      val h4 = runner.install (H4) .pass
      val hs = Seq (h1, h2, h3, h4)
      for (h1 <- hs; h2 <- hs)
        h1.hail (h2.localId)
      h1.issueAtlas (settled (h1, h2, h3)) .pass

      val cb = runner.setup (h1, h2) .capture()
      scheduler.run (count = target, timers = true)
      h1.issueAtlas (issuing (h1, h2, h3) (h1, h2, h4)) .pass
      val count = scheduler.run (timers = !cb.wasInvoked)
      cb.passedOrTimedout

      runner.verify (hs) .pass

      count
    }

  def for3replacing1 [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val average = forTargets (
        for3hosts (_) (init)) (
            for3replacing1 (_, _) (init))
    info (s"Average time with three hosts replacing one: ${average}ms")
  }

  def for3replacing1withSourceBouncing [H <: Host] (
      seed: Long,
      target1: Int,
      target3: Int
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Unit =

    s"for3replacing1withSourceBouncing (${seed}L, $target1, $target3, $config)"
    .withRandomScheduler (seed, init) { kit =>
      import kit._

      val target2 = random.nextInt (target3)
      val d3 = new StubDiskDrive

      val h1 = runner.install (H1) .pass
      val h2 = runner.install (H2) .pass
      var h3 = runner.install (H3, d3) .pass
      val h4 = runner.install (H4) .pass
      var hs = Seq (h1, h2, h3, h4)
      for (h1 <- hs; h2 <- hs)
        h1.hail (h2.localId)
      h1.issueAtlas (settled (h1, h2, h3)) .pass

      val cb = runner.setup (h1, h2) .capture()
      scheduler.run (count = target1, timers = true)
      h1.issueAtlas (issuing (h1, h2, h3) (h1, h2, h4)) .pass
      scheduler.run (count = target2, timers = true)
      h3.shutdown()
      scheduler.run (count = target3 - target2, timers = true)
      val cb2 = runner.reboot (H3, d3) .capture()
      scheduler.run (timers = !cb.wasInvoked || !cb2.wasInvoked)
      cb.passedOrTimedout
      h3 = cb2.passed

      hs = Seq (h1, h2, h3, h4)
      runner.verify (hs) .pass
    }

  def for3replacing1withSourceBouncing [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val average = forDoubleTargets (
        for3hosts (_) (init)) (
            for3replacing1 (_, _) (init)) (
                for3replacing1withSourceBouncing (_, _, _) (init))
    info (s"Average time with three hosts replacing one, a source host bounces: ${average}ms")
  }

  def for3replacing1withTargetBouncing [H <: Host] (
      seed: Long,
      target1: Int,
      target3: Int
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Unit =

    s"for3replacing1withTargetBouncing (${seed}L, $target1, $target3, $config)"
    .withRandomScheduler (seed, init) { kit =>
      import kit._

      val target2 = random.nextInt (target3)
      val d4 = new StubDiskDrive

      val h1 = runner.install (H1) .pass
      val h2 = runner.install (H2) .pass
      val h3 = runner.install (H3) .pass
      var h4 = runner.install (H4, d4) .pass
      var hs = Seq (h1, h2, h3, h4)
      for (h1 <- hs; h2 <- hs)
        h1.hail (h2.localId)
      h1.issueAtlas (settled (h1, h2, h3)) .pass

      val cb = runner.setup (h1, h2) .capture()
      scheduler.run (count = target1, timers = true)
      h1.issueAtlas (issuing (h1, h2, h3) (h1, h2, h4)) .pass
      scheduler.run (count = target2, timers = true)
      h4.shutdown()
      scheduler.run (count = target3 - target2, timers = true)
      val cb2 = runner.reboot (H4, d4) .capture()
      scheduler.run (timers = !cb.wasInvoked || !cb2.wasInvoked)
      cb.passedOrTimedout
      h4 = cb2.passed

      hs = Seq (h1, h2, h3, h4)
      runner.verify (hs) .pass
    }

  def for3replacing1withTargetBouncing [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val average = forDoubleTargets (
        for3hosts (_) (init)) (
            for3replacing1 (_, _) (init)) (
                for3replacing1withTargetBouncing (_, _, _) (init))
    info (s"Average time with three hosts replacing one, a target host bounces: ${average}ms")
  }

  def for3replacing1withCommonBouncing [H <: Host] (
      seed: Long,
      target1: Int,
      target3: Int
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Unit =

    s"for3replacing1withCommonBouncing (${seed}L, $target1, $target3, $config)"
    .withRandomScheduler (seed, init) { kit =>
      import kit._

      val target2 = random.nextInt (target3)
      val d2 = new StubDiskDrive

      val h1 = runner.install (H1) .pass
      var h2 = runner.install (H2, d2) .pass
      val h3 = runner.install (H3) .pass
      val h4 = runner.install (H4) .pass
      var hs = Seq (h1, h2, h3, h4)
      for (h1 <- hs; h2 <- hs)
        h1.hail (h2.localId)
      h1.issueAtlas (settled (h1, h2, h3)) .pass

      val cb = runner.setup (h1, h3) .capture()
      scheduler.run (count = target1, timers = true)
      h1.issueAtlas (issuing (h1, h2, h3) (h1, h2, h4)) .pass
      scheduler.run (count = target2, timers = true)
      h2.shutdown()
      scheduler.run (count = target3 - target2, timers = true)
      val cb2 = runner.reboot (H2, d2) .capture()
      scheduler.run (timers = !cb.wasInvoked || !cb2.wasInvoked)
      cb.passedOrTimedout
      h2 = cb2.passed

      hs = Seq (h1, h2, h3, h4)
      runner.verify (hs) .pass
    }

  def for3replacing1withCommonBouncing [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val average = forDoubleTargets (
        for3hosts (_) (init)) (
            for3replacing1 (_, _) (init)) (
                for3replacing1withCommonBouncing (_, _, _) (init))
    info (s"Average time with three hosts replacing one, a common host bounces: ${average}ms")
  }

  def for3replacing2 [H <: Host] (
      seed: Long,
      target: Int
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Int =

    s"for3replacing2 (${seed}L, $target, $config)"
    .withRandomScheduler (seed, init) { kit =>
      import kit._

      val h1 = runner.install (H1) .pass
      val h2 = runner.install (H2) .pass
      val h3 = runner.install (H3) .pass
      val h4 = runner.install (H4) .pass
      val h5 = runner.install (H5) .pass
      val hs = Seq (h1, h2, h3, h4, h5)
      for (h1 <- hs; h2 <- hs)
        h1.hail (h2.localId)
      h1.issueAtlas (settled (h1, h2, h3)) .pass

      val cb = runner.setup (h1, h2) .capture()
      scheduler.run (count = target, timers = true)
      h1.issueAtlas (issuing (h1, h2, h3) (h1, h4, h5)) .pass
      val count = scheduler.run (timers = !cb.wasInvoked)
      cb.passedOrTimedout

      runner.verify (hs) .pass

      count
    }

  def for3replacing2 [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val average = forTargets (
        for3hosts (_) (init)) (
            for3replacing2 (_, _) (init))
    info (s"Average time with three hosts replacing two: ${average}ms")
  }

  def for3replacing2withSourceBouncing [H <: Host] (
      seed: Long,
      target1: Int,
      target3: Int
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Unit =

    s"for3replacing2withSourceBouncing (${seed}L, $target1, $target3, $config)"
    .withRandomScheduler (seed, init) { kit =>
      import kit._

      val target2 = random.nextInt (target3)
      val d3 = new StubDiskDrive

      val h1 = runner.install (H1) .pass
      val h2 = runner.install (H2) .pass
      var h3 = runner.install (H3, d3) .pass
      val h4 = runner.install (H4) .pass
      val h5 = runner.install (H5) .pass
      var hs = Seq (h1, h2, h3, h4, h5)
      for (h1 <- hs; h2 <- hs)
        h1.hail (h2.localId)
      h1.issueAtlas (settled (h1, h2, h3)) .pass

      val cb = runner.setup (h1, h2) .capture()
      scheduler.run (count = target1, timers = true)
      h1.issueAtlas (issuing (h1, h2, h3) (h1, h4, h5)) .pass
      scheduler.run (count = target2, timers = true)
      h3.shutdown()
      scheduler.run (count = target3 - target2, timers = true)
      val cb2 = runner.reboot (H3, d3) .capture()
      scheduler.run (timers = !cb.wasInvoked || !cb2.wasInvoked)
      cb.passedOrTimedout
      h3 = cb2.passed

      hs = Seq (h1, h2, h3, h4, h5)
      runner.verify (hs) .pass
    }

  def for3replacing2withSourceBouncing [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val average = forDoubleTargets (
        for3hosts (_) (init)) (
            for3replacing2 (_, _) (init)) (
                for3replacing2withSourceBouncing (_, _, _) (init))
    info (s"Average time with three hosts replacing two, a source host bounces: ${average}ms")
  }

  def for3replacing2withTargetBouncing [H <: Host] (
      seed: Long,
      target1: Int,
      target3: Int
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Unit =

    s"for3replacing2withTargetBouncing (${seed}L, $target1, $target3, $config)"
    .withRandomScheduler (seed, init) { kit =>
      import kit._

      val target2 = random.nextInt (target3)
      val d4 = new StubDiskDrive

      val h1 = runner.install (H1) .pass
      val h2 = runner.install (H2) .pass
      val h3 = runner.install (H3) .pass
      var h4 = runner.install (H4, d4) .pass
      val h5 = runner.install (H5) .pass
      var hs = Seq (h1, h2, h3, h4, h5)
      for (h1 <- hs; h2 <- hs)
        h1.hail (h2.localId)
      h1.issueAtlas (settled (h1, h2, h3)) .pass

      val cb = runner.setup (h1, h2) .capture()
      scheduler.run (count = target1, timers = true)
      h1.issueAtlas (issuing (h1, h2, h3) (h1, h4, h5)) .pass
      scheduler.run (count = target2, timers = true)
      h4.shutdown()
      scheduler.run (count = target3 - target2, timers = true)
      val cb2 = runner.reboot (H4, d4) .capture()
      scheduler.run (timers = !cb.wasInvoked || !cb2.wasInvoked)
      cb.passedOrTimedout
      h4 = cb2.passed

      hs = Seq (h1, h2, h3, h4, h5)
      runner.verify (hs) .pass
    }

  def for3replacing2withTargetBouncing [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val average = forDoubleTargets (
        for3hosts (_) (init)) (
            for3replacing2 (_, _) (init)) (
                for3replacing2withTargetBouncing (_, _, _) (init))
    info (s"Average time with three hosts replacing two, a target host bounces: ${average}ms")
  }

  def for3replacing2withCommonBouncing [H <: Host] (
      seed: Long,
      target1: Int,
      target3: Int
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Unit =

    s"for3replacing2withCommonBouncing (${seed}L, $target1, $target3, $config)"
    .withRandomScheduler (seed, init) { kit =>
      import kit._

      val target2 = random.nextInt (target3)
      val d1 = new StubDiskDrive

      var h1 = runner.install (H1, d1) .pass
      val h2 = runner.install (H2) .pass
      val h3 = runner.install (H3) .pass
      val h4 = runner.install (H4) .pass
      val h5 = runner.install (H5) .pass
      var hs = Seq (h1, h2, h3, h4, h5)
      for (h1 <- hs; h2 <- hs)
        h1.hail (h2.localId)
      h1.issueAtlas (settled (h1, h2, h3)) .pass

      val cb = runner.setup (h2, h3) .capture()
      scheduler.run (count = target1, timers = true)
      h1.issueAtlas (issuing (h1, h2, h3) (h1, h4, h5)) .pass
      scheduler.run (count = target2, timers = true)
      h1.shutdown()
      scheduler.run (count = target3 - target2, timers = true)
      val cb2 = runner.reboot (H1, d1) .capture()
      scheduler.run (timers = !cb.wasInvoked || !cb2.wasInvoked)
      cb.passedOrTimedout
      h1 = cb2.passed

      hs = Seq (h1, h2, h3, h4, h5)
      runner.verify (hs) .pass
    }

  def for3replacing2withCommonBouncing [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val average = forDoubleTargets (
        for3hosts (_) (init)) (
            for3replacing2 (_, _) (init)) (
                for3replacing2withCommonBouncing (_, _, _) (init))
    info (s"Average time with three hosts replacing two, a common host bounces: ${average}ms")
  }

  def for3to3 [H <: Host] (
      seed: Long,
      target: Int
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Int =

    s"for3to3 (${seed}L, $target, $config)"
    .withRandomScheduler (seed, init) { kit =>
      import kit._

      var h1 = runner.install (H1) .pass
      val h2 = runner.install (H2) .pass
      val h3 = runner.install (H3) .pass
      val h4 = runner.install (H4) .pass
      val h5 = runner.install (H5) .pass
      val h6 = runner.install (H6) .pass
      var hs = Seq (h1, h2, h3, h4, h5, h6)
      for (h1 <- hs; h2 <- hs)
        h1.hail (h2.localId)
      h1.issueAtlas (settled (h1, h2, h3)) .pass

      val cb = runner.setup (h1, h4) .capture()
      scheduler.run (count = target, timers = true)
      h1.issueAtlas (issuing (h1, h2, h3) (h4, h5, h6)) .pass
      val count = scheduler.run (timers = !cb.wasInvoked)
      cb.passedOrTimedout

      runner.verify (hs) .pass

      count
    }

  def for3to3 [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val average = forTargets (
        for3hosts (_) (init)) (
            for3to3 (_, _) (init))
    info (s"Average time with three hosts moving: ${average}ms")
  }

  def for3to3withSourceBouncing [H <: Host] (
      seed: Long,
      target1: Int,
      target3: Int
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Unit =

    s"for3to3withSourceBouncing (${seed}L, $target1, $target3, $config)"
    .withRandomScheduler (seed, init) { kit =>
      import kit._

      val target2 = random.nextInt (target3)
      val d3 = new StubDiskDrive

      val h1 = runner.install (H1) .pass
      val h2 = runner.install (H2) .pass
      var h3 = runner.install (H3, d3) .pass
      val h4 = runner.install (H4) .pass
      val h5 = runner.install (H5) .pass
      val h6 = runner.install (H6) .pass
      var hs = Seq (h1, h2, h3, h4, h5, h6)
      for (h1 <- hs; h2 <- hs)
        h1.hail (h2.localId)
      h1.issueAtlas (settled (h1, h2, h3)) .pass

      val cb = runner.setup (h1, h4) .capture()
      scheduler.run (count = target1, timers = true)
      h1.issueAtlas (issuing (h1, h2, h3) (h4, h5, h6)) .pass
      scheduler.run (count = target2, timers = true)
      h3.shutdown()
      scheduler.run (count = target3 - target2, timers = true)
      val cb2 = runner.reboot (H3, d3) .capture()
      scheduler.run (timers = !cb.wasInvoked || !cb2.wasInvoked)
      cb.passedOrTimedout
      h3 = cb2.passed

      hs = Seq (h1, h2, h3, h4, h5, h6)
      runner.verify (hs) .pass
    }

  def for3to3withSourceBouncing [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val average = forDoubleTargets (
        for3hosts (_) (init)) (
            for3to3 (_, _) (init)) (
                for3to3withSourceBouncing (_, _, _) (init))
    info (s"Average time with three hosts moving, a source host bounces: ${average}ms")
  }

  def for3to3withTargetBouncing [H <: Host] (
      seed: Long,
      target1: Int,
      target3: Int
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Unit =

    s"for3to3withTargetBouncing (${seed}L, $target1, $target3, $config)"
    .withRandomScheduler (seed, init) { kit =>
      import kit._

      val target2 = random.nextInt (target3)
      val d6 = new StubDiskDrive

      val h1 = runner.install (H1) .pass
      val h2 = runner.install (H2) .pass
      val h3 = runner.install (H3) .pass
      val h4 = runner.install (H4) .pass
      val h5 = runner.install (H5) .pass
      var h6 = runner.install (H6, d6) .pass
      var hs = Seq (h1, h2, h3, h4, h5, h6)
      for (h1 <- hs; h2 <- hs)
        h1.hail (h2.localId)
      h1.issueAtlas (settled (h1, h2, h3)) .pass

      val cb = runner.setup (h1, h4) .capture()
      scheduler.run (count = target1, timers = true)
      h1.issueAtlas (issuing (h1, h2, h3) (h4, h5, h6)) .pass
      scheduler.run (count = target2, timers = true)
      h6.shutdown()
      scheduler.run (count = target3 - target2, timers = true)
      val cb2 = runner.reboot (H6, d6) .capture()
      scheduler.run (timers = !cb.wasInvoked || !cb2.wasInvoked)
      cb.passedOrTimedout
      h6 = cb2.passed

      hs = Seq (h1, h2, h3, h4, h5, h6)
      runner.verify (hs) .pass
    }

  def for3to3withTargetBouncing [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val average = forDoubleTargets (
        for3hosts (_) (init)) (
            for3to3 (_, _) (init)) (
                for3to3withTargetBouncing (_, _, _) (init))
    info (s"Average time with three hosts moving, a target host bounces: ${average}ms")
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
    .withRandomScheduler (seed, init) { kit =>
      import kit._

      val hs =
        for (id <- Seq (H1, H2, H3, H4, H5, H6, H7, H8))
          yield runner.install (id) .pass
      val Seq (h1, h2, h3) = hs take 3
      for (h1 <- hs; h2 <- hs)
        h1.hail (h2.localId)
      val atlas1 = Seq (settled (h1, h2, h3))
      h1.issueAtlas (atlas1: _*) .pass

      val cb = runner.setup (h1, h2) .capture()
      scheduler.run (count = target, timers = true)
      h1.issueAtlas (rewriteFor3to8 (atlas1, hs): _*) .pass
      scheduler.run (timers = !cb.wasInvoked)
      cb.passedOrTimedout

      runner.verify (hs) .pass
    }

  def for3to8 [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val average = forTargets (
        for3hosts (_) (init)) (
            for3to8 (_, _) (init))
    info (s"Average time for 3 growing to 8: ${average}ms")
  }

  def for3to8MT [H <: Host] (
      target: Int
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Unit =

    s"for3to8MT ($target, $config)"
    .withMultithreadedScheduler (init) { kit =>
      import kit._

      val hs =
        for (id <- Seq (H1, H2, H3, H4, H5, H6, H7, H8))
          yield runner.install (id) .pass
      val Seq (h1, h2, h3) = hs take 3
      for (h1 <- hs; h2 <- hs)
        h1.hail (h2.localId)
      val atlas1 = Seq (settled (h1, h2, h3))
      h1.issueAtlas (atlas1: _*) .pass

      val start = System.currentTimeMillis
      scheduler.delay (target) {
        h1.issueAtlas (rewriteFor3to8 (atlas1, hs): _*) .run (disregard)
      }
      runner.setup (h1, h2) .passOrTimeout
      val end = System.currentTimeMillis

      runner.verify (hs) .await()

      (end - start).toInt
    }

  def for3to8MT [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val time = for3hostsMT (init)
    val target =  Random.nextInt ((time * 0.7).toInt) + (time * 0.1).toInt
    for3to8MT (target) (init)
  }

  def for8to3 [H <: Host] (
      seed: Long,
      target: Int
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ): Unit =

    s"for8to3 (${seed}L, $target, $config)"
    .withRandomScheduler (seed, init) { kit =>
      import kit._

      val hs =
        for (id <- Seq (H1, H2, H3, H4, H5, H6, H7, H8))
          yield runner.install (id) .pass
      val Seq (h1, h2, h3) = hs take 3
      for (h1 <- hs; h2 <- hs)
        h1.hail (h2.localId)
      val atlas1 = cohortsFor8 (hs)
      h1.issueAtlas (atlas1: _*) .pass

      val cb = runner.setup (h1, h2) .capture()
      scheduler.run (count = target, timers = true)
      h1.issueAtlas (rewriteFor8to3 (atlas1, hs): _*) .pass
      scheduler.run (timers = !cb.wasInvoked)
      cb.passedOrTimedout

      runner.verify (hs) .pass
    }

    def for8to3 [H <: Host] (
      init: Random => ForStoreClusterRunner [H]
  ) (implicit
      config: StoreTestConfig
  ) {
    val average = forTargets (
        for8hosts (_) (init)) (
            for8to3 (_, _) (init))
    info (s"Average time for 8 shrinking to 3: ${average}ms")
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
