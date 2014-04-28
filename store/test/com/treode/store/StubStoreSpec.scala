package com.treode.store

import java.util.concurrent.Executors
import scala.util.Random

import com.treode.async.{Async, Scheduler, StubScheduler}
import org.scalatest.FreeSpec

class StubStoreSpec extends FreeSpec with StoreBehaviors {

  private class TestableStubStore (implicit scheduler: Scheduler) extends TestableStore {

    private val delegate = new StubStore

    def read (rt: TxClock, ops: ReadOp*): Async [Seq [Value]] =
      delegate.read (rt, ops:_*)

    def write (ct: TxClock, ops: WriteOp*): Async [TxClock] =
      delegate.write (TxId (Random.nextLong, 0), ct, ops:_*)

    def expectCells (t: TableId) (expected: Cell*): Unit =
      assertResult (expected.sorted) (delegate.scan (t))
  }

  "The StubStore should" - {

    behave like aStore (implicit scheduler => new TestableStubStore)

    behave like aMultithreadableStore (10000) {
      val executor = Executors.newScheduledThreadPool (4)
      val scheduler = StubScheduler.multithreaded (executor)
      new TestableStubStore () (scheduler)
    }}}
