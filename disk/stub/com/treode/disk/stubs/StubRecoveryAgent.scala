package com.treode.disk.stubs

import java.nio.file.{Path, Paths}
import java.util.concurrent.ExecutorService

import com.treode.async.{Async, Scheduler}
import com.treode.async.io.File
import com.treode.async.io.stubs.StubFile
import com.treode.disk.{Disks, DisksConfig, DiskGeometry, RecordDescriptor, RecoveryAgent}

import Async.guard
import Disks.Launch
import StubDisks.StubRecovery

private class StubRecoveryAgent (
    segmentBits: Int,
    diskBytes: Int
) (implicit
    scheduler: Scheduler
) extends StubRecovery {

  private implicit val config =
      DisksConfig (
          cell = 0,
          superBlockBits = 12,
          maximumRecordBytes = 1<<10,
          maximumPageBytes = 1 << (segmentBits - 2),
          checkpointBytes = Int.MaxValue,
          checkpointEntries = Int.MaxValue,
          cleaningFrequency = Int.MaxValue,
          cleaningLoad = 1)

  private val geom =
    DiskGeometry (
        segmentBits = segmentBits,
        blockBits = 6,
        diskBytes = diskBytes)

  private val delegate =
    Disks .recover () (scheduler, config) .asInstanceOf [RecoveryAgent]

  def replay [R] (desc: RecordDescriptor[R]) (f: R => Any): Unit =
    delegate.replay (desc) (f)

  def reattach (disk: StubDiskDrive): Async [Launch] = {
    disk.file = StubFile (disk.file.data)
    delegate._reattach ((Paths.get ("a"), disk.file)) .map (new StubLaunchAgent (_))
  }

  def attach (disk: StubDiskDrive): Async [Launch] = {
    disk.file = StubFile (diskBytes)
    delegate._attach ((Paths.get ("a"), disk.file, geom)) .map (new StubLaunchAgent (_))
  }

  def _reattach (items: (Path, File)*): Async [Launch] = ???
    guard (throw new UnsupportedOperationException ("The StubDisks do not use files."))

  def reattach (exec: ExecutorService, items: Path*): Async [Launch] =
    guard (throw new UnsupportedOperationException ("The StubDisks do not use files."))

  def _attach (items: (Path, File, DiskGeometry)*): Async [Launch] = ???
    guard (throw new UnsupportedOperationException ("The StubDisks do not use files."))

  def attach (exec: ExecutorService, items: (Path, DiskGeometry)*): Async [Launch] =
    guard (throw new UnsupportedOperationException ("The StubDisks do not use files."))
}
