package com.treode.store.stubs

import java.util.concurrent.Executors
import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.async.stubs.{AsyncChecks, StubScheduler}
import com.treode.store._
import org.scalatest.FreeSpec

class StubStoreSpec extends FreeSpec with AsyncChecks with StoreBehaviors {

  private class TestableStubStore (implicit kit: StoreTestKit) extends TestableStore {
    import kit.scheduler

    private val delegate = new StubStore

    def read (rt: TxClock, ops: ReadOp*): Async [Seq [Value]] =
      delegate.read (rt, ops:_*)

    def write (ct: TxClock, ops: WriteOp*): Async [TxClock] =
      delegate.write (TxId (Random.nextLong, 0), ct, ops:_*)

    def expectCells (t: TableId) (expected: Cell*): Unit =
      assertResult (expected.sorted) (delegate.scan (t))
  }

  "The StubStore should" - {

    behave like aStore (implicit kit => new TestableStubStore)

    behave like aMultithreadableStore (10000) { implicit kit =>
      new TestableStubStore
    }}}
