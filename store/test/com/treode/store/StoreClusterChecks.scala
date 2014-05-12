package com.treode.store

import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.async.stubs.{AsyncChecks, StubScheduler}
import com.treode.async.stubs.implicits._
import com.treode.cluster.HostId
import com.treode.cluster.stubs.StubNetwork
import com.treode.disk.stubs.StubDiskDrive
import org.scalatest.{Assertions, Informing, Suite}

import StoreClusterChecks.{Host, Package}
import StoreTestTools._

trait StoreClusterChecks extends AsyncChecks {
  this: Suite with Informing =>

  private val ncrashes =
    intensity match {
      case "development" => 1
      case _ => 10
    }

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

  def forThreeStableHosts [H <: Host] (
      seed: Long,
      checkpoint: Double,
      compaction: Double,
      messageFlakiness: Double
  ) (
      init: Random => ForStoreClusterRunner [H]
  ): Int = {

    implicit val random = new Random (seed)
    val runner = init (random)

    try {

      implicit val kit = StoreTestKit.random (random)
      import kit.{network, scheduler}

      val h1 = runner .install (H1, checkpoint, compaction) .pass
      val h2 = runner .install (H2, checkpoint, compaction) .pass
      val h3 = runner .install (H3, checkpoint, compaction) .pass
      for (h <- Seq (h1, h2, h3))
        h.setAtlas (settled (h1, h2, h3))
      network.messageFlakiness = messageFlakiness

      val cb = runner.setup (scheduler) (h1, h2) .capture()
      val count = scheduler.run (timers = !cb.wasInvoked)
      cb.passed
      runner.asserts foreach (_ ())
      runner.recover (scheduler) (h1) .pass

      count

    } catch {
      case t: Throwable =>
        info (s"forThreeStableHosts (${seed}L, $checkpoint, $compaction, $messageFlakiness)")
        runner.messages foreach (info (_))
        throw t
    }}

  def forThreeStableHosts [H <: Host] (
      checkpoint: Double,
      compaction: Double,
      messageFlakiness: Double
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) {
    val start = System.currentTimeMillis
    for (_ <- 0 until nseeds) {
      val seed = Random.nextLong()
      forThreeStableHosts (seed, checkpoint, compaction, messageFlakiness) (init)
    }
    val end = System.currentTimeMillis
    val average = (end - start) / nseeds
    info (s"Average time on three stable hosts: ${average}ms")
  }

  def forOneHostCrashing [H <: Host] (
      seed: Long,
      target: Int,
      checkpoint: Double,
      compaction: Double,
      messageFlakiness: Double
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) {

    implicit val random = new Random (seed)
    val runner = init (random)

    try {
      implicit val kit = StoreTestKit.random (random)
      import kit.{network, scheduler}

      val d1 = new StubDiskDrive
      val d2 = new StubDiskDrive
      val d3 = new StubDiskDrive

      var h1 = runner .boot (H1, checkpoint, compaction, d1, true) .pass
      var h2 = runner .boot (H2, checkpoint, compaction, d2, true) .pass
      var h3 = runner .boot (H3, checkpoint, compaction, d3, true) .pass
      for (h <- Seq (h1, h2, h3))
        h.setAtlas (settled (h1, h2, h3))
      network.messageFlakiness = messageFlakiness

      val cb = runner.setup (scheduler) (h1, h2) .capture()
      scheduler.run (count = target, timers = true)
      h3.shutdown()
      scheduler.run (timers = !cb.wasInvoked, oblivious = true)
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

    } catch {
      case t: Throwable =>
        info (s"forOneHostCrashing (${seed}L, $target, $checkpoint, $compaction, $messageFlakiness)")
        runner.messages foreach (info (_))
        throw t
    }}

  def forOneHostCrashing [H <: Host] (
      seed: Long,
      checkpoint: Double,
      compaction: Double,
      messageFlakiness: Double
  ) (
      init: Random => ForStoreClusterRunner [H]
  ): Int = {

    // Run the first time for as long as possible.
    val count = forThreeStableHosts (seed, checkpoint, compaction, messageFlakiness) (init)

    // Run the subsequent times for a portion of what was possible.
    if (count < ncrashes) {
      for (i <- 1 to count)
        forOneHostCrashing (seed, i, checkpoint, compaction, messageFlakiness) (init)
      count
    } else {
      val random = new Random (seed)
      for (i <- 1 to ncrashes) {
        val target =  random.nextInt (count - 1) + 1
        forOneHostCrashing (seed, target, checkpoint, compaction, messageFlakiness) (init)
      }
      ncrashes
    }}

  def forOneHostCrashing [H <: Host] (
      checkpoint: Double,
      compaction: Double,
      messageFlakiness: Double
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) {
    val start = System.currentTimeMillis
    var count = 0
    for (_ <- 0 until nseeds / ncrashes) {
      val seed = Random.nextLong()
      count += forOneHostCrashing (seed, checkpoint, compaction, messageFlakiness) (init)
    }
    val end = System.currentTimeMillis
    val average = (end - start) / count
    info (s"Average time with one host crashing: ${average}ms")
  }

  def forOneHostCrashed [H <: Host] (
      seed: Long,
      checkpoint: Double,
      compaction: Double,
      messageFlakiness: Double
  ) (
      init: Random => ForStoreClusterRunner [H]
  ): Int = {

    implicit val random = new Random (seed)
    val runner = init (random)

    try {

      implicit val kit = StoreTestKit.random (random)
      import kit.{network, scheduler}

      val d1 = new StubDiskDrive
      val d2 = new StubDiskDrive

      var h1 = runner .boot (H1, checkpoint, compaction, d1, true) .pass
      var h2 = runner .boot (H2, checkpoint, compaction, d2, true) .pass
      val h3 = runner .install (H3, checkpoint, compaction) .pass
      h3.shutdown()
      for (h <- Seq (h1, h2, h3))
        h.setAtlas (settled (h1, h2, h3))
      network.messageFlakiness = messageFlakiness

      val cb = runner.setup (scheduler) (h1, h2) .capture()
      val count = scheduler.run (timers = !cb.wasInvoked, oblivious = true)
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

    } catch {
      case t: Throwable =>
        info (s"forOneHostCrashed (${seed}L, $checkpoint, $compaction, $messageFlakiness)")
        runner.messages foreach (info (_))
        throw t
    }}

  def forOneHostRebooting [H <: Host] (
      seed: Long,
      target: Int,
      checkpoint: Double,
      compaction: Double,
      messageFlakiness: Double
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) {

    implicit val random = new Random (seed)
    val runner = init (random)

    try {
      implicit val kit = StoreTestKit.random (random)
      import kit.{network, scheduler}

      val d3 = new StubDiskDrive

      val h1 = runner .install (H1, checkpoint, compaction) .pass
      val h2 = runner .install (H2, checkpoint, compaction) .pass
      val h3 = runner .boot (H3, checkpoint, compaction, d3, true) .pass
      h3.shutdown()
      for (h <- Seq (h1, h2, h3))
        h.setAtlas (settled (h1, h2, h3))
      network.messageFlakiness = messageFlakiness

      val cb = runner.setup (scheduler) (h1, h2) .capture()
      scheduler.run (count = target, timers = true, oblivious = true)
      val cb2 = runner .boot (H3, checkpoint, compaction, d3, false) .capture()
      scheduler.run (timers = !cb.wasInvoked, oblivious = true)
      assert (!cb2.hasFailed)
      runner.asserts foreach (_ ())

      network.messageFlakiness = 0.0
      runner .recover (scheduler) (h1) .pass

    } catch {
      case t: Throwable =>
        info (s"forOneHostRebooting (${seed}L, $target, $checkpoint, $compaction, $messageFlakiness)")
        runner.messages foreach (info (_))
        throw t
    }}

  def forOneHostRebooting [H <: Host] (
      seed: Long,
      checkpoint: Double,
      compaction: Double,
      messageFlakiness: Double
  ) (
      init: Random => ForStoreClusterRunner [H]
  ): Int = {

    // Run the first time for as long as possible.
    val count = forOneHostCrashed (seed, checkpoint, compaction, messageFlakiness) (init)

    // Run the subsequent times for a portion of what was possible.
    if (count < ncrashes) {
      for (i <- 1 to count)
        forOneHostRebooting (seed, i, checkpoint, compaction, messageFlakiness) (init)
      count
    } else {
      val random = new Random (seed)
      for (i <- 1 to ncrashes) {
        val target =  random.nextInt (count - 1) + 1
        forOneHostRebooting (seed, target, checkpoint, compaction, messageFlakiness) (init)
      }
      ncrashes
    }}

  def forOneHostRebooting [H <: Host] (
      checkpoint: Double,
      compaction: Double,
      messageFlakiness: Double
  ) (
      init: Random => ForStoreClusterRunner [H]
  ) {
    val start = System.currentTimeMillis
    var count = 0
    for (_ <- 0 until nseeds / ncrashes) {
      val seed = Random.nextLong()
      count += forOneHostRebooting (seed, checkpoint, compaction, messageFlakiness) (init)
    }
    val end = System.currentTimeMillis
    val average = (end - start) / count
    info (s"Average time with one host rebooting: ${average}ms")
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
