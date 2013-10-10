package com.treode

import com.treode.cluster.concurrent.Callback
import com.treode.cluster.events.Events
import com.treode.pickle.Pickler

package store {

  trait ReadCallback extends Callback [Seq [Value]]

  trait Transaction {
    def ft: TxClock
    def commit (wt: TxClock)
    def abort()
  }

  trait WriteCallback extends Callback [Transaction] {
    def advance()
    def conflicts (ks: Set [Int])
  }

  trait Store {
    def read (batch: ReadBatch, cb: ReadCallback)
    def write (batch: WriteBatch, cb: WriteCallback)
  }}

package object store {

  private [store] implicit class StoreEvents (events: Events) {

    def exceptionAbortedRead (e: Throwable): Unit =
      events.warning (s"Aborting read due to exception", e)

    def exceptionAbortedAudit (id: TxId, e: Throwable): Unit =
      events.warning (s"Aborting audit $id due to exception", e)
  }}
