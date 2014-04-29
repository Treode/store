package com.treode.store.locks

import org.scalamock.scalatest.MockFactory
import org.scalatest.WordSpec

import com.treode.async.Async
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.store.{Bytes, StoreConfig, StoreTestTools, TxClock}
import com.treode.pickle.Picklers

import StoreTestTools._

class LockSpec extends WordSpec with MockFactory {

  private implicit class RichLockSpace (space: LockSpace) {

    def read (rt: Int, k1: String, ks: String*): Async [Unit] =
      space.read (rt, (k1 +: ks) .map (_.hashCode))

    def write (ft: Int, k1: String, ks: String*): Async [LockSet] =
      space.write (ft, (k1 +: ks) .map (_.hashCode))
  }

  def assertClock (expected: Long) (actual: Option [TxClock]): Unit =
    assertResult (Some (new TxClock (expected))) (actual)

  "A Lock" when {

    "not previously held" should {

      "grant a reader immediately rather than invoke grant later" in {
        val lock = new Lock
        val r = mock [LockReader]
        (r.rt _) .expects() .returns (0) .anyNumberOfTimes()
        (r.grant _) .expects() .never()
        assertResult (true) (lock.read (r))
      }

      "grant a writer immediately rather than invoke grant later" in {
        val lock = new Lock
        val w = mock [LockWriter]
        (w.ft _) .expects() .returns (0) .anyNumberOfTimes()
        (w.grant _) .expects (TxClock.zero) .never()
        assertResult (Some (TxClock.zero)) (lock.write (w))
      }}

    "previously held by a reader" should {

      "grant a writer immediately and not invoke the callback" in {
        val lock = new Lock
        val r = mock [LockReader]
        (r.rt _) .expects() .returns (1) .anyNumberOfTimes()
        (r.grant _) .expects() .never()
        assertResult (true) (lock.read (r))
        val w = mock [LockWriter]
        (w.ft _) .expects() .returns (0) .anyNumberOfTimes()
        (w.grant _) .expects (TxClock.zero) .never()
        assertClock (1) (lock.write (w))
      }}

    "currently held by a writer" should {

      "grant an earlier reader immediately" in {
        val lock = new Lock
        val w = mock [LockWriter]
        (w.ft _) .expects() .returns (1) .anyNumberOfTimes()
        (w.grant _) .expects (TxClock.zero) .never()
        assertClock (1) (lock.write (w))
        val r = mock [LockReader]
        (r.rt _) .expects() .returns (0) .anyNumberOfTimes()
        (r.grant _) .expects() .never()
        assertResult (true) (lock.read (r))
        lock.release (w)
      }

      "hold a later reader until release" in {
        val lock = new Lock
        val w = mock [LockWriter]
        (w.ft _) .expects() .returns (1) .anyNumberOfTimes()
        (w.grant _) .expects (TxClock.zero) .never()
        assertClock (1) (lock.write (w))
        val r = mock [LockReader]
        (r.rt _) .expects() .returns (2) .anyNumberOfTimes()
        (r.grant _) .expects() .never()
        assertResult (false) (lock.read (r))
        (r.grant _) .expects() .once()
        lock.release (w)
      }

      "release all readers at once" in {
        val lock = new Lock
        val w = mock [LockWriter]
        (w.ft _) .expects() .returns (1) .anyNumberOfTimes()
        (w.grant _) .expects (TxClock.zero) .never()
        assertClock (1) (lock.write (w))
        val r1 = mock [LockReader]
        (r1.rt _) .expects() .returns (2) .anyNumberOfTimes()
        (r1.grant _) .expects() .never()
        assertResult (false) (lock.read (r1))
        val r2 = mock [LockReader]
        (r2.rt _) .expects() .returns (2) .anyNumberOfTimes()
        (r2.grant _) .expects() .never()
        assertResult (false) (lock.read (r2))
        (r1.grant _) .expects() .once()
        (r2.grant _) .expects() .once()
        lock.release (w)
      }

      "hold the second writer until release" in {
        val lock = new Lock
        val w1 = mock [LockWriter]
        (w1.ft _) .expects() .returns (0) .anyNumberOfTimes()
        (w1.grant _) .expects (TxClock.zero) .never()
        assertClock (0) (lock.write (w1))
        val w2 = mock [LockWriter]
        (w2.ft _) .expects() .returns (0) .anyNumberOfTimes()
        (w2.grant _) .expects (TxClock.zero) .never()
        assertResult (None) (lock.write (w2))
        (w2.grant _) .expects (TxClock.zero) .once()
        lock.release (w1)
      }

      "release only one writer" in {
        val lock = new Lock
        val w1 = mock [LockWriter]
        (w1.ft _) .expects() .returns (0) .anyNumberOfTimes()
        (w1.grant _) .expects (TxClock.zero) .never()
        assertClock (0) (lock.write (w1))
        val w2 = mock [LockWriter]
        (w2.ft _) .expects() .returns (0) .anyNumberOfTimes()
        (w2.grant _) .expects (TxClock.zero) .never()
        assertResult (None) (lock.write (w2))
        val w3 = mock [LockWriter]
        (w3.ft _) .expects() .returns (0) .anyNumberOfTimes()
        (w3.grant _) .expects (TxClock.zero) .never()
        assertResult (None) (lock.write (w3))
        (w2.grant _) .expects (TxClock.zero) .once()
        lock.release (w1)
      }}}

  "A set of locks" should {

    val Apple = "apple"
    val Banana = "banana"
    val Orange = "orange"

    "acquire all locks before proceeding" in {
      implicit val scheduler = StubScheduler.random()
      implicit val config = TestStoreConfig (lockSpaceBits = 8)
      val locks = new LockSpace
      val w1 = locks.write (1, Apple, Banana) .pass
      val w2 = locks.write (2, Banana, Orange) .capture()
      w2.assertNotInvoked()
      val r3 = locks.read (3, Apple, Orange) .capture()
      w2.assertNotInvoked()
      r3.assertNotInvoked()
      w1.release()
      r3.assertNotInvoked()
      w2.passed.release()
      r3.passed
    }}}
