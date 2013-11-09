package com.treode.store.local

import com.treode.pickle.Picklers
import com.treode.store._
import org.scalatest.Assertions

trait TestableLocalStore extends LocalStore with Assertions {

  private val Xid = TxId (Bytes (Picklers.int, 1))

  def table (id: TableId): TestableTimedTable

  def readAndExpect (rt: TxClock, ops: ReadOp*) (expected: Value*) {
    val batch = ReadBatch (rt, ops)
    read (batch, new StubReadCallback {
      override def pass (actual: Seq [Value]) = expectResult (expected) (actual)
    })
  }

  def writeAndCommit (ct: TxClock, ops: WriteOp*): TxClock = {
    val batch = WriteBatch (Xid, ct, ct, ops)
    var ts = TxClock.Zero
    write (batch, new StubWriteCallback {
      override def pass (tx: Transaction) {
        ts = tx.ft + 7 // Leave gaps in the timestamps
        tx.commit (ts)
      }})
    ts
  }

  def writeAndAbort (ct: TxClock, ops: WriteOp*) {
    val batch = WriteBatch (Xid, ct, ct, ops)
    write (batch, new StubWriteCallback {
      override def pass (tx: Transaction) = tx.abort()
    })
  }

  def writeExpectAdvance (ct: TxClock, ops: WriteOp*) = {
    val batch = WriteBatch (Xid, ct, ct, ops)
    write (batch, new StubWriteCallback {
      override def advance() = ()
    })
  }

  def writeExpectConflicts (ct: TxClock, ops: WriteOp*) (expected: Int*) = {
    val batch = WriteBatch (Xid, ct, ct, ops)
    write (batch, new StubWriteCallback {
      override def conflicts (actual: Set [Int]) = expectResult (expected.toSet) (actual)
    })
  }}
