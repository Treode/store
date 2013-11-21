package com.treode.store

import org.scalatest.Assertions

import Assertions.expectResult

trait TestableStore {

  def read (rt: TxClock, ops: Seq [ReadOp], cb: ReadCallback)

  def write (ct: TxClock, ops: Seq [WriteOp], cb: WriteCallback)

  def expectCells (t: TableId) (expected: TimedCell*)

  def runTasks()

  def readAndExpect (rt: TxClock, ops: ReadOp*) (expected: Value*) {
    val cb = new ReadCaptor
    read (rt, ops, cb)
    runTasks()
    expectResult (expected) (cb.passed)
  }

  def writeExpectPass (ct: TxClock, ops: WriteOp*): TxClock = {
    val cb = new WriteCaptor
    write (ct, ops, cb)
    runTasks()
    cb.passed
  }

  def writeExpectAdvance (ct: TxClock, ops: WriteOp*) = {
    val cb = new WriteCaptor
    write (ct, ops, cb)
    runTasks()
    cb.advanced
  }

  def writeExpectCollisions (ct: TxClock, ops: WriteOp*) (expected: Int*) = {
    val cb = new WriteCaptor
    write (ct, ops, cb)
    runTasks()
    expectResult (expected.toSet) (cb.collided)
  }}
