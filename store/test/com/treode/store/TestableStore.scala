package com.treode.store

import com.treode.async.Async
import org.scalatest.Assertions

import Assertions.expectResult

trait TestableStore {

  def read (rt: TxClock, ops: ReadOp*): Async [Seq [Value]]

  def write (ct: TxClock, ops: WriteOp*): Async [WriteResult]

  def expectCells (t: TableId) (expected: Cell*)
}
