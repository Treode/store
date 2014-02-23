package com.treode.store.paxos

import java.nio.file.Paths
import com.treode.async.{AsyncTestTools, CallbackCaptor}
import com.treode.async.io.StubFile
import com.treode.cluster.{Cluster, HostId, StubActiveHost, StubNetwork}
import com.treode.disk.{Disks, DisksConfig, DiskGeometry}
import com.treode.store.StoreConfig

import AsyncTestTools._

private class StubPaxosHost (id: HostId, network: StubNetwork)
extends StubActiveHost (id, network) {
  import network.{random, scheduler}

  implicit val cluster: Cluster = this

  implicit val disksConfig = DisksConfig (14, 1<<24, 1<<16, 10, 1)
  implicit val storeConfig = StoreConfig (4, 1<<16)

  implicit val recovery = Disks.recover()
  val _paxos = Paxos.recover()

  val file = new StubFile
  val geometry = DiskGeometry (10, 6, 1<<20)
  val files = Seq ((Paths.get ("a"), file, geometry))

  val _launch =
    for {
      launch <- recovery.attach (files)
      paxos <- _paxos.launch (launch)
    } yield {
      launch.launch()
      (launch.disks, paxos)
    }

  val captor = _launch.capture()
  scheduler.runTasks()
  while (!captor.wasInvoked)
    Thread.sleep (10)
  implicit val (disks, paxos) = captor.passed

  val acceptors = paxos.asInstanceOf [PaxosKit] .acceptors
}
