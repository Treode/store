package com.treode.disk.stubs

import com.treode.async.{Async, Callback}
import com.treode.async.implicits._
import com.treode.disk._

private class StubReleaser (disk: StubDiskDrive) extends AbstractReleaser [Long] {

  private def free (released: Seq [Long]): Unit =
    disk.free (released)

  def leave (epoch: Int): Unit =
    free (_leave (epoch))

  def release (segments: Seq [Long]): Unit =
    free (_release (segments))

  def join [A] (cb: Callback [A]): Callback [A] = {
    val epoch = _join()
    cb.ensure (leave (epoch))
  }

  def join [A] (task: Async [A]): Async [A] =
    new Async [A] {
      def run (cb: Callback [A]): Unit = task.run (join (cb))
    }}
