package com.treode.disk

import java.util.ArrayList

import com.treode.async.{AsyncIterator, Callback, Scheduler, delay, guard}
import com.treode.pickle.PicklerRegistry

import PicklerRegistry.TaggedFunction

private class RecoveryAgent (
    records: RecordRegistry,
    loaders: ReloadRegistry,
    launches: ArrayList [Launch => Any],
    val cb: Callback [Disks]) (
        implicit val scheduler: Scheduler) {

  def launch (disks: DiskDrives): Unit =
    guard (cb) {
      new LaunchAgent (disks, launches, cb)
    }

  def recover (roots: Position, disks: DiskDrives) {

    val logsReplayed = delay (cb) { _: Unit =>
      launch (disks)
    }

    val rootsReloaded = delay (cb) { _: Unit =>
      disks.replay (records, logsReplayed)
    }

    val rootsRead = delay (cb) { roots: Seq [Reload => Any] =>
      new ReloadAgent (disks, roots, rootsReloaded)
    }

    val pager = CheckpointRegistry.pager (loaders.pickler)
    disks.fetch (pager, roots, rootsRead)
  }}
