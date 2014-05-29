package com.treode.store.atomic

import scala.collection.JavaConversions
import scala.util.Random

import com.treode.async.stubs.implicits._
import com.treode.cluster.stubs.StubNetwork
import com.treode.disk.stubs.{CrashChecks, StubDiskDrive}
import com.treode.store._
import org.scalatest.{Informing, Suite}

import AtomicTestTools._
import JavaConversions._

trait AtomicBehaviors extends CrashChecks with StoreClusterChecks {
  this: Suite with Informing =>

  private [atomic] def crashAndRecover (
      nbatches: Int,
      ntables: Int,
      nkeys: Int,
      nwrites: Int,
      nops: Int
  ) (implicit
      random: Random,
      config: StoreTestConfig
  ) = {

    val tracker = new AtomicTracker
    val disk = new StubDiskDrive

    crash.info (s"crashAndRecover ($nbatches, $ntables, $nkeys, $nwrites, $nops, $config)")

    .setup { implicit scheduler =>
      implicit val network = StubNetwork (random)
      for {
        host <- StubAtomicHost.boot (H1, disk, true)
        _ = host.setAtlas (settled (host))
        _ <- tracker.batches (nbatches, ntables, nkeys, nwrites, nops, host)
      } yield ()
    }

    .recover { implicit scheduler =>
      implicit val network = StubNetwork (random)
      val host = StubAtomicHost.boot (H1, disk, false) .pass
      host.setAtlas (settled (host))
      scheduler.run (timers = !host.atomic.writers.deputies.isEmpty)
      tracker.check (host) .pass
    }}}
