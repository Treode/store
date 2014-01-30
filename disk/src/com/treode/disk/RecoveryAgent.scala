package com.treode.disk

import java.util.ArrayList

import com.treode.async.{Callback, Scheduler, delay, guard}

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

    disks.fetch (loaders.pager, roots, rootsRead)
  }}
