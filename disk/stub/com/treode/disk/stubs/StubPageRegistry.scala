package com.treode.disk.stubs

import java.util.concurrent.ConcurrentHashMap
import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.async.implicits._
import com.treode.async.misc.EpochReleaser
import com.treode.disk._

import Async.guard
import PageLedger.{Groups, Merger}

private class StubPageRegistry (releaser: EpochReleaser) (implicit
    random: Random,
    scheduler: Scheduler,
    disk: StubDiskDrive,
    config: StubDisk.Config
) extends AbstractPageRegistry {

  import config.compactionProbability

  def probe (iter: Iterator [(Long, StubPage)]): Async [(Groups, Seq [Long])] = {
    val merger = new Merger
    val segments = Seq.newBuilder [Long]
    iter.async.foreach { case (offset, page) =>
      val groups = Set (PageGroup (page.group))
      for {
        live <- probe (page.typ, page.obj, groups)
      } yield {
        if (live._2.isEmpty) {
          releaser.release (disk.free (Seq (offset)))
        } else if (compactionProbability > 0.0 && random.nextDouble < compactionProbability) {
          merger.add (Map ((page.typ, page.obj) -> groups))
          segments += offset
        }
      }
    } .map { _ =>
      (merger.result, segments.result)
    }}}
