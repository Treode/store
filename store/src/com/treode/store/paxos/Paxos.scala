package com.treode.store.paxos

import scala.util.Random

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.cluster.Cluster
import com.treode.disk.Disks
import com.treode.store.{Bytes, StoreConfig}
import com.treode.store.catalog.CohortCatalog

trait Paxos {

  def lead (key: Bytes, value: Bytes): Async [Bytes]

  def propose (key: Bytes, value: Bytes): Async [Bytes]
}

object Paxos {

  trait Recovery {

    def launch (implicit launch: Disks.Launch, cohorts: CohortCatalog): Async [Paxos]
  }

  def recover() (implicit
      random: Random,
      scheduler: Scheduler,
      cluster: Cluster,
      recover: Disks.Recovery,
      config: StoreConfig
  ): Recovery =
    new RecoveryKit
}
