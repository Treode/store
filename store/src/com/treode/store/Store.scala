package com.treode.store

import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.cluster.Cluster
import com.treode.disk.Disks

trait Store {

  def read (rt: TxClock, ops: Seq [ReadOp]): Async [Seq [Value]]

  def write (xid: TxId, ct: TxClock, ops: Seq [WriteOp]): Async [WriteResult]
}

object Store {

  trait Recovery {

    def launch (launch: Disks.Launch): Async [Store]
  }

  def recover() (implicit
      random: Random,
      scheduler: Scheduler,
      cluster: Cluster,
      recovery: Disks.Recovery,
      config: StoreConfig
  ): Recovery =
    new RecoveryKit
}
