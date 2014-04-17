package com.treode.store.atomic

import com.treode.async.{Async, StubScheduler}
import com.treode.store.{Bytes, StoreTestTools, TxClock}
import com.treode.store.locks.LockSet
import org.scalatest.Assertions

import Assertions.fail

private trait AtomicTestTools extends StoreTestTools {


  implicit class RichPrepareResult (actual: Async [PrepareResult]) {
    import PrepareResult._

    def expectPrepared (implicit s: StubScheduler): (TxClock, LockSet) =
      actual.pass match {
        case Prepared (vt, locks) =>
          (vt, locks)
        case _ =>
          fail (s"Expected Written, found ${actual}")
          throw new Exception
      }

    def expectCollided (ks: Int*) (implicit s: StubScheduler): Unit =
      actual.expect (Collided (ks))

    def expectStale (implicit s: StubScheduler): Unit =
      actual.expect (Stale)

    def abort() (implicit s: StubScheduler) {
      val (vt, locks) = expectPrepared
      locks.release()
    }}}

private object AtomicTestTools extends AtomicTestTools
