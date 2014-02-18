package com.treode.disk

import scala.collection.mutable.Builder
import com.treode.async.{Async, Callback}

private class Releaser {

  private class Epoch (var parties: Int, val segments: Seq [SegmentPointer]) {
    override def toString = s"Epoch($parties, $segments)"
  }

  private var epochs = Map.empty [Int, Epoch]
  private var epoch = 0
  private var parties = 0

  def join(): Int = synchronized {
    parties += 1
    epoch
  }

  private def leave (epoch: Int, builder: Builder [SegmentPointer, _]) {
    if (epoch == this.epoch) {
      parties -= 1
    } else {
      epochs.get (epoch) match {
        case Some (past) =>
          past.parties -= 1
          if (past.parties == 0) {
            epochs -= epoch
            builder ++= past.segments
            leave (epoch+1, builder)
          }
        case None =>
          throw new AssertionError ("Party left forgotten epoch")
      }}}

  private def free (released: Seq [SegmentPointer]) {
    for ((disk, segs) <- released groupBy (_.disk))
      disk.free (segs)
  }

  def _leave (epoch: Int): Seq [SegmentPointer] = synchronized {
    val builder = Seq.newBuilder [SegmentPointer]
    leave (epoch, builder)
    builder.result
  }

  def leave (epoch: Int): Unit =
    free (_leave (epoch))

  def join [A] (cb: Callback [A]): Callback [A] =
    new Callback [A] {
      val epoch = join()
      def pass (v: A) {
        leave (epoch)
        cb.pass (v)
      }
      def fail (t: Throwable) {
        leave (epoch)
        cb.fail (t)
      }}

  def join [A] (task: Async [A]): Async [A] =
    new Async [A] {
      def run (cb: Callback [A]): Unit = task.run (join (cb))
    }

  def _release (segments: Seq [SegmentPointer]): Seq [SegmentPointer] = synchronized {
    if (epochs.isEmpty && parties == 0) {
      segments
    } else {
      epochs += epoch -> new Epoch (parties, segments)
      epoch += 1
      parties = 1
      Seq.empty [SegmentPointer]
    }}

  def release (segments: Seq [SegmentPointer]): Unit =
    free (_release (segments))
}
