package com.treode.store.cluster.atomic

import com.treode.concurrent.{CallbackCaptor, StubScheduler}
import com.treode.store.{Bytes, SimpleAccessor, TxId}
import org.scalatest.Assertions

private class TestableMainDb (db: SimpleAccessor [Bytes, DeputyStatus], scheduler: StubScheduler)
extends Assertions {

  def get (xid: TxId): Option [DeputyStatus] = {
    val cb = new CallbackCaptor [Option [DeputyStatus]]
    db.get (xid.id, cb)
    scheduler.runTasks()
    cb.passed
  }

  def expectCommitted (xid: TxId) =
    expectResult (Some (DeputyStatus.Committed)) (get (xid))

  def expectAborted (xid: TxId) =
    expectResult (Some (DeputyStatus.Aborted)) (get (xid))
}
