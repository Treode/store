package com.treode.store.local

import com.treode.pickle.Picklers
import com.treode.store._
import org.scalatest.Assertions

trait TestableLocalStore extends LocalStore with Assertions {

  private val Xid = TxId (Bytes (Picklers.int, 1))

  def table (id: TableId): TestableTimedTable

  def readAndExpect (rt: Int, ops: ReadOp*) (expected: Value*) {
    val batch = ReadBatch (rt, ops)
    read (batch, new StubReadCallback {
      override def pass (actual: Seq [Value]) = expectResult (expected) (actual)
    })
  }

  def writeExpectApply (ct: Int, ops: WriteOp*) (cb: Transaction => Any) = {
    val batch = WriteBatch (Xid, ct, ct, ops)
    write (batch, new StubWriteCallback {
      override def pass (tx: Transaction) = cb (tx)
    })
  }

  def writeExpectAdvance (ct: Int, ops: WriteOp*) = {
    val batch = WriteBatch (Xid, ct, ct, ops)
    write (batch, new StubWriteCallback {
      override def advance() = ()
    })
  }

  def writeExpectConflicts (ct: Int, ops: WriteOp*) (expected: Int*) = {
    val batch = WriteBatch (Xid, ct, ct, ops)
    write (batch, new StubWriteCallback {
      override def conflicts (actual: Set [Int]) = expectResult (expected.toSet) (actual)
    })
  }}
