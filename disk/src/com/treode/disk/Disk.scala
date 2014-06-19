package com.treode.disk

import java.nio.file.Path
import com.treode.async.{Async, Callback, Scheduler}

trait Disk {

  def record [R] (desc: RecordDescriptor [R], entry: R): Async [Unit]

  def read [P] (desc: PageDescriptor [_, P], pos: Position): Async [P]

  def write [G, P] (desc: PageDescriptor [G, P], obj: ObjectId, group: G, page: P): Async [Position]

  def compact (desc: PageDescriptor [_, _], obj: ObjectId): Async [Unit]

  def join [A] (task: Async [A]): Async [A]
}

object Disk {

  case class Config (
      checkpointBytes: Int,
      checkpointEntries: Int,
      cleaningFrequency: Int,
      cleaningLoad: Int,
      maximumRecordBytes: Int,
      maximumPageBytes: Int,
      pageCacheEntries: Int,
      superBlockBits: Int
  ) {

    require (
        checkpointBytes > 0,
        "The checkpoint interval must be more than 0 bytes.")

    require (
        checkpointEntries > 0,
        "The checkpoint interval must be more than 0 entries.")

    require (
        cleaningFrequency > 0,
        "The cleaning interval must be more than 0 segments.")

    require (
        cleaningLoad > 0,
        "The cleaning load must be more than 0 segemnts.")

    require (
        maximumRecordBytes > 0,
        "The maximum record size must be more than 0 bytes.")

    require (
        maximumPageBytes > 0,
        "The maximum page size must be more than 0 bytes.")

    require (
        pageCacheEntries > 0,
        "The size of the page cache must be more than 0 entries.")

    require (
        superBlockBits > 0,
        "A superblock must have more than 0 bytes.")

    val superBlockBytes = 1 << superBlockBits
    val superBlockMask = superBlockBytes - 1
    val diskLeadBytes = 1 << (superBlockBits + 1)

    val minimumSegmentBits = {
      val bytes = math.max (maximumRecordBytes, maximumPageBytes)
      Integer.SIZE - Integer.numberOfLeadingZeros (bytes - 1) + 1
    }

    def checkpoint (bytes: Int, entries: Int): Boolean =
      bytes > checkpointBytes || entries > checkpointEntries

    def clean (segments: Int): Boolean =
      segments >= cleaningFrequency
  }

  object Config {

    val suggested = Config (
        checkpointBytes = 1<<24,
        checkpointEntries = 10000,
        cleaningFrequency = 7,
        cleaningLoad = 1,
        maximumRecordBytes = 1<<24,
        maximumPageBytes = 1<<24,
        pageCacheEntries = 10000,
        superBlockBits = 14)
  }

  trait Controller {

    implicit def disk: Disk

    def drives: Async [Seq [DriveDigest]]

    def attach (items: DriveAttachment*): Async [Unit]

    def drain (items: Path*): Async [Unit]

    def shutdown(): Async [Unit]
  }

  trait Launch {

    implicit def disk: Disk

    implicit def controller: Controller

    def sysid: Array [Byte]

    def checkpoint (f: => Async [Unit])

    def handle [G] (desc: PageDescriptor [G, _], handler: PageHandler [G])

    def launch()
  }

  trait Recovery {

    def replay [R] (desc: RecordDescriptor [R]) (f: R => Any)

    def reattach (items: Path*): Async [Launch]
  }

  def init (
      sysid: Array [Byte],
      superBlockBits: Int,
      segmentBits: Int,
      blockBits: Int,
      diskBytes: Long,
      paths: Path*
  ): Unit =
    DiskDrive.init (sysid, superBlockBits, segmentBits, blockBits, diskBytes, paths)

  def recover () (implicit scheduler: Scheduler, config: Disk.Config): Recovery =
    new RecoveryAgent
}
