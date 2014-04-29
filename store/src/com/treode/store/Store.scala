package com.treode.store

import scala.util.Random

import com.treode.async.{Async, AsyncIterator, Scheduler}
import com.treode.cluster.Cluster
import com.treode.disk.Disks

trait Store {

  def read (rt: TxClock, ops: ReadOp*): Async [Seq [Value]]

  def write (xid: TxId, ct: TxClock, ops: WriteOp*): Async [TxClock]

  def status (xid: TxId): Async [TxStatus]

  def scan (table: TableId, key: Bytes, time: TxClock): AsyncIterator [Cell]
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
