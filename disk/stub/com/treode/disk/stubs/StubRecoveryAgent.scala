package com.treode.disk.stubs

import java.nio.file.{Path, Paths}
import scala.util.Random

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.async.io.File
import com.treode.async.io.stubs.StubFile
import com.treode.async.misc.EpochReleaser
import com.treode.disk._

import Async.{guard, supply}
import Callback.ignore
import Disk.Launch
import StubDisk.StubRecovery

private class StubRecoveryAgent (implicit
    random: Random,
    scheduler: Scheduler,
    config: StubDiskConfig
) extends StubRecovery {

  private val records = new RecordRegistry
  private var open = true

  def requireOpen(): Unit =
    require (open, "Recovery has already begun.")

  def replay [R] (desc: RecordDescriptor [R]) (f: R => Any): Unit =
    synchronized {
      requireOpen()
      records.replay (desc) (f)
    }

  def reattach (drive: StubDiskDrive): Async [Launch] =
    guard {
      synchronized {
        requireOpen()
        open = false
      }
      for {
        _ <- drive.replay (records)
      } yield {
        val releaser = new EpochReleaser
        val disk = new StubDisk (releaser) (random, scheduler, drive, config)
        new StubLaunchAgent (releaser, disk) (random, scheduler, drive, config)
      }}

  def attach (drive: StubDiskDrive): Async [Launch] =
    supply {
      synchronized {
        requireOpen()
        open = false
      }
      val releaser = new EpochReleaser
      val disk = new StubDisk (releaser) (random, scheduler, drive, config)
      new StubLaunchAgent (releaser, disk) (random, scheduler, drive, config)
    }

  def reattach (items: Path*): Async [Launch] =
    guard (throw new UnsupportedOperationException ("The StubDisk do not use files."))

  def attach (items: (Path, DriveGeometry)*): Async [Launch] =
    guard (throw new UnsupportedOperationException ("The StubDisk do not use files."))
}
