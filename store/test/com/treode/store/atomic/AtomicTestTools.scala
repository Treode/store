package com.treode.store.atomic

import com.treode.async.{Async, StubScheduler}
import com.treode.store.{Bytes, TimedTestTools, TxClock}
import com.treode.store.locks.LockSet
import org.scalatest.Assertions

import Assertions.fail

private trait AtomicTestTools extends TimedTestTools {

  implicit class RichWriteDeputy (s: WriteDeputy) {

    def isRestoring = classOf [WriteDeputy#Restoring] .isInstance (s.state)
    def isOpen = classOf [WriteDeputy#Open] .isInstance (s.state)
    def isPrepared = classOf [WriteDeputy#Prepared] .isInstance (s.state)
    def isCommitted = classOf [WriteDeputy#Committed] .isInstance (s.state)
    def isAborted = classOf [WriteDeputy#Aborted] .isInstance (s.state)
    def isShutdown = classOf [WriteDeputy#Shutdown] .isInstance (s.state)
  }

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
