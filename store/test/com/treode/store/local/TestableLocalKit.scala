package com.treode.store.local

import com.treode.concurrent.CallbackCaptor
import com.treode.pickle.Picklers
import com.treode.store._
import org.scalatest.Assertions

import Assertions._

private trait TestableLocalKit {
  this: LocalKit =>

  private val Xid = TxId (Bytes (Picklers.int, 1))

  def getTimedTable (id: TableId): TestableTimedTable

  def readAndExpect (rt: TxClock, ops: ReadOp*) (expected: Value*) {
    val batch = ReadBatch (rt, ops)
    val cb = new ReadCaptor
    read (batch, cb)
    expectResult (expected) (cb.passed)
  }

  def prepareAndCommit (ct: TxClock, ops: WriteOp*): TxClock = {
    val batch = WriteBatch (Xid, ct, ct, ops)
    val cb1 = new PrepareCaptor
    prepare (batch, cb1)
    val tx = cb1.passed
    val wt = tx.ft + 7 // Leave gaps in time.
    val cb2 = new CallbackCaptor [Unit]
    tx.commit (wt, cb2)
    cb2.passed
    wt
  }

  def prepareAndAbort (ct: TxClock, ops: WriteOp*) {
    val batch = WriteBatch (Xid, ct, ct, ops)
    val cb = new PrepareCaptor
    prepare (batch, cb)
    val tx = cb.passed
    tx.abort()
  }

  def prepareExpectAdvance (ct: TxClock, ops: WriteOp*) = {
    val batch = WriteBatch (Xid, ct, ct, ops)
    val cb = new PrepareCaptor
    prepare (batch, cb)
    cb.advanced
  }

  def prepareExpectCollisions (ct: TxClock, ops: WriteOp*) (expected: Int*) = {
    val batch = WriteBatch (Xid, ct, ct, ops)
    val cb = new PrepareCaptor
    prepare (batch, cb)
    expectResult (expected.toSet) (cb.collided)
  }}
