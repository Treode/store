package com.treode.store.lock

import org.scalamock.scalatest.MockFactory
import org.scalatest.WordSpec

import com.treode.store.{Bytes,TxClock}
import com.treode.pickle.Picklers

class LockSpec extends WordSpec with MockFactory {

  private implicit class RichLockSpace (space: LockSpace) {

    def acquire (k1: String, ks: String*): LockSet =
      space.acquire ((k1 +: ks) map (k => Bytes (Picklers.string, k)))
  }

  "A Lock" when {

    "not previously held" should {

      "grant a reader immediately and not invoke the callback" in {
        val lock = new Lock
        val r = mock [ReadAcquisition]
        (r.rt _) .expects() .returns (0) .anyNumberOfTimes()
        (r.grant _) .expects() .never()
        expectResult (true) (lock.read (r))
      }

      "grant a writer immediately and not invoke the callback" in {
        val lock = new Lock
        val w = mock [WriteAcquisition]
        (w.ft _) .expects() .returns (0) .anyNumberOfTimes()
        (w.grant _) .expects (TxClock.Zero) .never()
        expectResult (Some (TxClock.Zero)) (lock.write (w))
      }}

    "previously held by a reader" should {

      "grant a writer immediately and not invoke the callback" in {
        val lock = new Lock
        val r = mock [ReadAcquisition]
        (r.rt _) .expects() .returns (1) .anyNumberOfTimes()
        (r.grant _) .expects() .never()
        expectResult (true) (lock.read (r))
        val w = mock [WriteAcquisition]
        (w.ft _) .expects() .returns (0) .anyNumberOfTimes()
        (w.grant _) .expects (TxClock.Zero) .never()
        expectResult (Some (TxClock (1))) (lock.write (w))
      }}

    "currently held by a writer" should {

      "grant an earlier reader immediately" in {
        val lock = new Lock
        val w = mock [WriteAcquisition]
        (w.ft _) .expects() .returns (1) .anyNumberOfTimes()
        (w.grant _) .expects (TxClock (1)) .never()
        expectResult (Some (TxClock (1))) (lock.write (w))
        val r = mock [ReadAcquisition]
        (r.rt _) .expects() .returns (0) .anyNumberOfTimes()
        (r.grant _) .expects() .never()
        expectResult (true) (lock.read (r))
        lock.release()
      }

      "hold a later reader until release" in {
        val lock = new Lock
        val w = mock [WriteAcquisition]
        (w.ft _) .expects() .returns (1) .anyNumberOfTimes()
        (w.grant _) .expects (TxClock (1)) .never()
        expectResult (Some (TxClock (1))) (lock.write (w))
        val r = mock [ReadAcquisition]
        (r.rt _) .expects() .returns (2) .anyNumberOfTimes()
        (r.grant _) .expects() .never()
        expectResult (false) (lock.read (r))
        (r.grant _) .expects() .once()
        lock.release()
      }

      "release all readers at once" in {
        val lock = new Lock
        val w = mock [WriteAcquisition]
        (w.ft _) .expects() .returns (1) .anyNumberOfTimes()
        (w.grant _) .expects (TxClock (1)) .never()
        expectResult (Some (TxClock (1))) (lock.write (w))
        val r1 = mock [ReadAcquisition]
        (r1.rt _) .expects() .returns (2) .anyNumberOfTimes()
        (r1.grant _) .expects() .never()
        expectResult (false) (lock.read (r1))
        val r2 = mock [ReadAcquisition]
        (r2.rt _) .expects() .returns (2) .anyNumberOfTimes()
        (r2.grant _) .expects() .never()
        expectResult (false) (lock.read (r2))
        (r1.grant _) .expects() .once()
        (r2.grant _) .expects() .once()
        lock.release()
      }

      "hold the second writer until release" in {
        val lock = new Lock
        val w1 = mock [WriteAcquisition]
        (w1.ft _) .expects() .returns (0) .anyNumberOfTimes()
        (w1.grant _) .expects (TxClock.Zero) .never()
        expectResult (Some (TxClock.Zero)) (lock.write (w1))
        val w2 = mock [WriteAcquisition]
        (w2.ft _) .expects() .returns (0) .anyNumberOfTimes()
        (w2.grant _) .expects (TxClock.Zero) .never()
        expectResult (None) (lock.write (w2))
        (w2.grant _) .expects (TxClock.Zero) .once()
        lock.release()
      }

      "release only one writer" in {
        val lock = new Lock
        val w1 = mock [WriteAcquisition]
        (w1.ft _) .expects() .returns (0) .anyNumberOfTimes()
        (w1.grant _) .expects (TxClock.Zero) .never()
        expectResult (Some (TxClock.Zero)) (lock.write (w1))
        val w2 = mock [WriteAcquisition]
        (w2.ft _) .expects() .returns (0) .anyNumberOfTimes()
        (w2.grant _) .expects (TxClock.Zero) .never()
        expectResult (None) (lock.write (w2))
        val w3 = mock [WriteAcquisition]
        (w3.ft _) .expects() .returns (0) .anyNumberOfTimes()
        (w3.grant _) .expects (TxClock.Zero) .never()
        expectResult (None) (lock.write (w3))
        (w2.grant _) .expects (TxClock.Zero) .once()
        lock.release()
      }}}

  "A set of locks" should {

    val Apple = "apple"
    val Banana = "banana"
    val Orange = "orange"

    "acquire all locks before proceeding" in {
      val locks = new LockSpace (4)
      val w1 = locks.acquire (Apple, Banana)
      val cb1 = mock [TxClock => Unit]
      (cb1.apply _) .expects (TxClock (1)) .once()
      w1.write (1) (cb1)
      val w2 = locks.acquire (Banana, Orange)
      val cb2 = mock [TxClock => Unit]
      w2.write (2) (cb2)
      val r3 = locks.acquire (Apple, Orange)
      val cb3 = mock [Unit => Unit]
      r3.read (3) (cb3())
      (cb2.apply _) .expects (TxClock (2)) .once()
      w1.release()
      (cb3.apply _) .expects() .once()
      w2.release()
    }}}
