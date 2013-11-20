package com.treode.store.local

import scala.util.Random

import com.treode.concurrent.{Callback, CallbackCaptor}
import com.treode.store._
import org.scalatest.Assertions

import Assertions._

private object LocalTimedTestTools extends TimedTestTools {

  implicit class RichCellIterator (iter: TimedIterator) {

    def toSeq: Seq [TimedCell] = {
      val builder = Seq.newBuilder [TimedCell]
      val loop = new Callback [TimedCell] {
        def pass (cell: TimedCell) {
          builder += cell
          if (iter.hasNext)
            iter.next (this)
        }
        def fail (t: Throwable) = throw t
      }
      if (iter.hasNext)
        iter.next (loop)
      builder.result
    }}

  implicit class RichLocalStore (s: LocalStore) {

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
    }}}
