package com.treode.store

import com.treode.async.Async
import org.scalatest.Assertions

import Assertions.assertResult

trait TestableStore {

  def read (rt: TxClock, ops: ReadOp*): Async [Seq [Value]]

  def write (ct: TxClock, ops: WriteOp*): Async [TxClock]

  def expectCells (t: TableId) (expected: Cell*)
}
