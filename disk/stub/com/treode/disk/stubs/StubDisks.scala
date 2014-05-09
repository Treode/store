package com.treode.disk.stubs

import com.treode.async.{Async, Scheduler}
import com.treode.disk.Disks

import Disks.{Launch, Recovery}

object StubDisks {

  trait StubRecovery extends Recovery {

    def reattach (disk: StubDiskDrive): Async [Launch]

    def attach (disk: StubDiskDrive): Async [Launch]
  }

  def recover (
      segmentBits: Int = 12,
      diskBytes: Int = 1<<20
  ) (implicit
      scheduler: Scheduler
  ): StubRecovery =
    new StubRecoveryAgent (segmentBits, diskBytes)
}
