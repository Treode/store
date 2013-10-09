package com.treode.store.lock

import org.scalamock.scalatest.MockFactory
import org.scalatest.WordSpec

import com.treode.store.{Bytes,TxClock}
import com.treode.pickle.Picklers

class LockSpec extends WordSpec with MockFactory {

  private implicit class RichLockSpace (space: LockSpace) {

    def read (rt: Int, k1: String, ks: String*) (cb: => Any): Unit =
      space.read (rt, (k1 +: ks) .map (_.hashCode)) (cb)

    def write (ft: Int, k1: String, ks: String*) (cb: LockSet => Any): Unit =
      space.write (ft, (k1 +: ks) .map (_.hashCode)) (cb)
  }

  "A Lock" when {

    "not previously held" should {

      "grant a reader immediately rather than invoke grant later" in {
        val lock = new Lock
        val r = mock [Reader]
        (r.rt _) .expects() .returns (0) .anyNumberOfTimes()
        (r.grant _) .expects() .never()
        expectResult (true) (lock.read (r))
      }

      "grant a writer immediately rather than invoke grant later" in {
        val lock = new Lock
        val w = mock [Writer]
        (w.ft _) .expects() .returns (0) .anyNumberOfTimes()
        (w.grant _) .expects (TxClock.Zero) .never()
        expectResult (Some (TxClock.Zero)) (lock.write (w))
      }}

    "previously held by a reader" should {

      "grant a writer immediately and not invoke the callback" in {
        val lock = new Lock
        val r = mock [Reader]
        (r.rt _) .expects() .returns (1) .anyNumberOfTimes()
        (r.grant _) .expects() .never()
        expectResult (true) (lock.read (r))
        val w = mock [Writer]
        (w.ft _) .expects() .returns (0) .anyNumberOfTimes()
        (w.grant _) .expects (TxClock.Zero) .never()
        expectResult (Some (TxClock (1))) (lock.write (w))
      }}

    "currently held by a writer" should {

      "grant an earlier reader immediately" in {
        val lock = new Lock
        val w = mock [Writer]
        (w.ft _) .expects() .returns (1) .anyNumberOfTimes()
        (w.grant _) .expects (TxClock (1)) .never()
        expectResult (Some (TxClock (1))) (lock.write (w))
        val r = mock [Reader]
        (r.rt _) .expects() .returns (0) .anyNumberOfTimes()
        (r.grant _) .expects() .never()
        expectResult (true) (lock.read (r))
        lock.release (w)
      }

      "hold a later reader until release" in {
        val lock = new Lock
        val w = mock [Writer]
        (w.ft _) .expects() .returns (1) .anyNumberOfTimes()
        (w.grant _) .expects (TxClock (1)) .never()
        expectResult (Some (TxClock (1))) (lock.write (w))
        val r = mock [Reader]
        (r.rt _) .expects() .returns (2) .anyNumberOfTimes()
        (r.grant _) .expects() .never()
        expectResult (false) (lock.read (r))
        (r.grant _) .expects() .once()
        lock.release (w)
      }

      "release all readers at once" in {
        val lock = new Lock
        val w = mock [Writer]
        (w.ft _) .expects() .returns (1) .anyNumberOfTimes()
        (w.grant _) .expects (TxClock (1)) .never()
        expectResult (Some (TxClock (1))) (lock.write (w))
        val r1 = mock [Reader]
        (r1.rt _) .expects() .returns (2) .anyNumberOfTimes()
        (r1.grant _) .expects() .never()
        expectResult (false) (lock.read (r1))
        val r2 = mock [Reader]
        (r2.rt _) .expects() .returns (2) .anyNumberOfTimes()
        (r2.grant _) .expects() .never()
        expectResult (false) (lock.read (r2))
        (r1.grant _) .expects() .once()
        (r2.grant _) .expects() .once()
        lock.release (w)
      }

      "hold the second writer until release" in {
        val lock = new Lock
        val w1 = mock [Writer]
        (w1.ft _) .expects() .returns (0) .anyNumberOfTimes()
        (w1.grant _) .expects (TxClock.Zero) .never()
        expectResult (Some (TxClock.Zero)) (lock.write (w1))
        val w2 = mock [Writer]
        (w2.ft _) .expects() .returns (0) .anyNumberOfTimes()
        (w2.grant _) .expects (TxClock.Zero) .never()
        expectResult (None) (lock.write (w2))
        (w2.grant _) .expects (TxClock.Zero) .once()
        lock.release (w1)
      }

      "release only one writer" in {
        val lock = new Lock
        val w1 = mock [Writer]
        (w1.ft _) .expects() .returns (0) .anyNumberOfTimes()
        (w1.grant _) .expects (TxClock.Zero) .never()
        expectResult (Some (TxClock.Zero)) (lock.write (w1))
        val w2 = mock [Writer]
        (w2.ft _) .expects() .returns (0) .anyNumberOfTimes()
        (w2.grant _) .expects (TxClock.Zero) .never()
        expectResult (None) (lock.write (w2))
        val w3 = mock [Writer]
        (w3.ft _) .expects() .returns (0) .anyNumberOfTimes()
        (w3.grant _) .expects (TxClock.Zero) .never()
        expectResult (None) (lock.write (w3))
        (w2.grant _) .expects (TxClock.Zero) .once()
        lock.release (w1)
      }}}

  "A set of locks" should {

    val Apple = "apple"
    val Banana = "banana"
    val Orange = "orange"

    "acquire all locks before proceeding" in {
      val locks = new LockSpace (4)
      var w1: LockSet = null
      locks.write (1, Apple, Banana) (w1 = _)
      assert (w1 != null)
      var w2: LockSet = null
      locks.write (2, Banana, Orange) (w2 = _)
      assert (w2 == null)
      val cb3 = mock [Unit => Unit]
      val r3 = locks.read (3, Apple, Orange) (cb3())
      w1.release()
      assert (w2 != null)
      (cb3.apply _) .expects() .once()
      w2.release()
    }}}
