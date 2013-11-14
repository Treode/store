package com.treode.store.local

import com.treode.pickle.Picklers
import com.treode.store._
import org.scalatest.Assertions

private trait TestableLocalStore extends Assertions {
  this: LocalStore =>

  private val Xid = TxId (Bytes (Picklers.int, 1))

  def table (id: TableId): TestableTimedTable

  def readAndExpect (rt: TxClock, ops: ReadOp*) (expected: Value*) {
    val batch = ReadBatch (rt, ops)
    read (batch, new StubReadCallback {
      override def pass (actual: Seq [Value]) = expectResult (expected) (actual)
    })
  }

  def prepareAndCommit (ct: TxClock, ops: WriteOp*): TxClock = {
    val batch = WriteBatch (Xid, ct, ct, ops)
    var ts = TxClock.zero
    prepare (batch, new StubPrepareCallback {
      override def pass (tx: Transaction) {
        ts = tx.ft + 7 // Leave gaps in the timestamps
        tx.commit (ts)
      }})
    ts
  }

  def prepareAndAbort (ct: TxClock, ops: WriteOp*) {
    val batch = WriteBatch (Xid, ct, ct, ops)
    prepare (batch, new StubPrepareCallback {
      override def pass (tx: Transaction) = tx.abort()
    })
  }

  def prepareExpectAdvance (ct: TxClock, ops: WriteOp*) = {
    val batch = WriteBatch (Xid, ct, ct, ops)
    prepare (batch, new StubPrepareCallback {
      override def advance() = ()
    })
  }

  def prepareExpectCollisions (ct: TxClock, ops: WriteOp*) (expected: Int*) = {
    val batch = WriteBatch (Xid, ct, ct, ops)
    prepare (batch, new StubPrepareCallback {
      override def collisions (actual: Set [Int]) = expectResult (expected.toSet) (actual)
    })
  }}
