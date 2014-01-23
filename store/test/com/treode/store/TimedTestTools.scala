package com.treode.store

import scala.util.Random

import com.treode.async.CallbackCaptor
import org.scalatest.Assertions

import Assertions.expectResult

private trait TimedTestTools {

  implicit class RichBytes (v: Bytes) {
    def ## (time: Int) = TimedCell (v, TxClock (time), None)
    def ## (time: TxClock) = TimedCell (v, time, None)
    def :: (time: Int) = Value (TxClock (time), Some (v))
    def :: (time: TxClock) = Value (time, Some (v))
    def :: (cell: TimedCell) = TimedCell (cell.key, cell.time, Some (v))
  }

  implicit class RichOption (v: Option [Bytes]) {
    def :: (time: Int) = Value (TxClock (time), v)
    def :: (time: TxClock) = Value (time, v)
  }

  implicit class RichTxClock (v: TxClock) {
    def + (n: Int) = TxClock (v.time + n)
    def - (n: Int) = TxClock (v.time - n)
  }

  implicit class RichTimedTable (t: TimedTable) {

    def getAndExpect (key: Bytes, time: TxClock) (expected: TimedCell) {
      val cb = new CallbackCaptor [TimedCell]
      t.get (key, time, cb)
      expectResult (expected) (cb.passed)
    }

    def putAndPass (key: Bytes, time: TxClock, value: Option [Bytes]) {
      val cb = new CallbackCaptor [Unit]
      t.put (key, time, value, cb)
      cb.passed
    }}

  implicit class RichPreparableStore (s: PreparableStore) {

    def readAndExpect (rt: TxClock, ops: ReadOp*) (expected: Value*) {
      val cb = new ReadCaptor
      s.read (rt, ops, cb)
      expectResult (expected) (cb.passed)
    }

    def prepareAndCommit (ct: TxClock, ops: WriteOp*): TxClock = {
      val cb1 = new PrepareCaptor
      s.prepare (ct, ops, cb1)
      val tx = cb1.passed
      val wt = tx.ft + 7 // Leave gaps in time.
      val cb2 = new CallbackCaptor [Unit]
      tx.commit (wt, cb2)
      cb2.passed
      wt
    }

    def prepareAndAbort (ct: TxClock, ops: WriteOp*) {
      val cb = new PrepareCaptor
      s.prepare (ct, ops, cb)
      val tx = cb.passed
      tx.abort()
    }

    def prepareExpectAdvance (ct: TxClock, ops: WriteOp*) = {
      val cb = new PrepareCaptor
      s.prepare (ct, ops, cb)
      cb.advanced
    }

    def prepareExpectCollisions (ct: TxClock, ops: WriteOp*) (expected: Int*) = {
      val cb = new PrepareCaptor
      s.prepare (ct, ops, cb)
      expectResult (expected.toSet) (cb.collided)
    }}

  def nextTable = TableId (Random.nextLong)

  def Get (id: TableId, key: Bytes): ReadOp =
    ReadOp (id, key)
}

private object TimedTestTools extends TimedTestTools
