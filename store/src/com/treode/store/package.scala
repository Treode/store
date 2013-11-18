package com.treode

import com.treode.concurrent.Callback
import com.treode.cluster.events.Events
import com.treode.pickle.Pickler

package store {

  trait ReadCallback extends Callback [Seq [Value]]

  trait WriteCallback extends Callback [TxClock] {
    def collisions (ks: Set [Int])
    def advance()
  }

  trait Store {
    def read (batch: ReadBatch, cb: ReadCallback)
    def write (batch: WriteBatch, cb: WriteCallback)
  }

  private trait Transaction {
    def ft: TxClock
    def commit (wt: TxClock, cb: Callback [Unit])
    def abort()
  }

  private trait PrepareCallback extends Callback [Transaction] {
    def collisions (ks: Set [Int])
    def advance()
  }

  private trait PreparableStore {
    def read (batch: ReadBatch, cb: ReadCallback)
    def prepare (batch: WriteBatch, cb: PrepareCallback)
    def commit (batch: WriteBatch, wt: TxClock, cb: Callback [Unit])
  }

  private trait SimpleStore {
    def openSimpleTable (id: TableId): SimpleTable
  }

  private trait LocalStore extends PreparableStore with SimpleStore

  private trait PaxosStore {
    def lead (key: Bytes, value: Bytes, cb: Callback [Bytes])
    def propose (key: Bytes, value: Bytes, cb: Callback [Bytes])
  }}

package object store {

  private [store] implicit class StoreEvents (events: Events) {

    def exceptionAbortedRead (e: Throwable): Unit =
      events.warning (s"Aborting read due to exception", e)

    def exceptionAbortedAudit (id: TxId, e: Throwable): Unit =
      events.warning (s"Aborting audit $id due to exception", e)
  }}
