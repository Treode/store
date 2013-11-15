package com.treode.store.local

import com.treode.concurrent.Callback
import com.treode.pickle.Picklers
import com.treode.store._
import org.scalatest.Assertions

private trait TestableLocalStore extends Assertions {
  this: LocalStore =>

  private val Xid = TxId (Bytes (Picklers.int, 1))

  def table (id: TableId): TestableTimedTable

  def readAndExpect (rt: TxClock, ops: ReadOp*) (expected: Value*) {
    val batch = ReadBatch (rt, ops)
    var actual: Seq [Value] = null
    read (batch, new StubReadCallback {
      override def pass (_actual: Seq [Value]) = actual = _actual
    })
    assert (actual != null, "Expected values.")
    expectResult (expected) (actual)
  }

  def prepareAndCommit (ct: TxClock, ops: WriteOp*): TxClock = {
    val batch = WriteBatch (Xid, ct, ct, ops)
    var ts = TxClock.zero
    prepare (batch, new StubPrepareCallback {
      override def pass (tx: Transaction) {
        ts = tx.ft + 7 // Leave gaps in the timestamps
        tx.commit (ts, Callback.ignore)
      }})
    assert (ts != TxClock.zero, "Write was not committed.")
    ts
  }

  def prepareAndAbort (ct: TxClock, ops: WriteOp*) {
    val batch = WriteBatch (Xid, ct, ct, ops)
    var mark = false
    prepare (batch, new StubPrepareCallback {
      override def pass (tx: Transaction) {
        tx.abort()
        mark = true
      }})
    assert (mark, "Write was not aborted.")
  }

  def prepareExpectAdvance (ct: TxClock, ops: WriteOp*) = {
    val batch = WriteBatch (Xid, ct, ct, ops)
    var mark = false
    prepare (batch, new StubPrepareCallback {
      override def advance() = mark = true
    })
    assert (mark, "Expected advance.")
  }

  def prepareExpectCollisions (ct: TxClock, ops: WriteOp*) (expected: Int*) = {
    val batch = WriteBatch (Xid, ct, ct, ops)
    var actual: Set [Int] = null
    prepare (batch, new StubPrepareCallback {
      override def collisions (_actual: Set [Int]) = actual = _actual
    })
    assert (actual != null, "Expected collisions.")
    expectResult (expected.toSet) (actual)
  }}
