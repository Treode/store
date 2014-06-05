package com.treode.store

import scala.util.Random

import com.treode.async.{Async, AsyncIterator, Scheduler}
import com.treode.cluster.Cluster
import com.treode.disk.Disk

trait Store {

  def read (rt: TxClock, ops: ReadOp*): Async [Seq [Value]]

  def write (xid: TxId, ct: TxClock, ops: WriteOp*): Async [TxClock]

  def status (xid: TxId): Async [TxStatus]

  def scan (table: TableId, start: Bound [Key], window: Window, slice: Slice): AsyncIterator [Cell]
}

object Store {

  trait Controller {

    implicit def store: Store

    def listen [C] (desc: CatalogDescriptor [C]) (f: C => Any)

    def issue [C] (desc: CatalogDescriptor [C]) (version: Int, cat: C)

    def issue (atlas: Atlas)
  }

  trait Recovery {

    def launch (launch: Disk.Launch): Async [Controller]
  }

  def recover() (implicit
      random: Random,
      scheduler: Scheduler,
      cluster: Cluster,
      recovery: Disk.Recovery,
      config: StoreConfig
  ): Recovery =
    new RecoveryKit
}
