package com.treode

import java.nio.file.{Path, StandardOpenOption}
import java.util.concurrent.ExecutorService

import com.treode.async.AsyncIterator
import com.treode.async.io.File

package disk {

  class AlreadyAttachedException (paths: Seq [Path]) extends Exception {
    override def getMessage = s"Disks already attached: ${paths mkString ", "}"
  }

  class DiskFullException extends Exception {
    override def getMessage = "DiskFull."
  }

  class ExtraDisksException (paths: Seq [Path]) extends Exception {
    override def getMessage = s"Extra disks in reattachment: ${paths mkString ", "}"
  }

  class InconsistentSuperBlocksException extends Exception {
    override def getMessage = "Inconsistent superblocks."
  }

  class NoSuperBlocksException extends Exception {
    override def getMessage = "No superblocks."
  }

  class MissingDisksException (paths: Seq [Path]) extends Exception {
    override def getMessage = s"Missing disks in reattachment: ${paths mkString ", "}"
  }

  class PanickedException (t: Throwable) extends Exception (t) {
    override def getMessage = "Panicked."
  }

  private case class SegmentBounds (num: Int, pos: Long, limit: Long)

  private case class SegmentPointer (disk: Int, num: Int)
}

package object disk {

  private [disk] type LogDispatcher = Dispatcher [PickledRecord]
  private [disk] type PageDispatcher = Dispatcher [PickledPage]
  private [disk] type ReplayIterator = AsyncIterator [(Long, Unit => Any)]

  private [disk] val SuperBlockBits = 14
  private [disk] val SuperBlockBytes = 1 << SuperBlockBits
  private [disk] val SuperBlockMask = SuperBlockBytes - 1
  private [disk] val DiskLeadBytes = 1 << (SuperBlockBits + 1)

  private [disk] implicit class RichIteratable [A] (iter: Seq [A]) {

    def mapBy [K] (k: A => K): Map [K, A] = {
      val b = Map.newBuilder [K, A]
      iter foreach (x => b += (k (x) -> x))
      b.result
    }

    def mapValuesBy [K, V] (k: A => K) (v: A => V): Map [K, V] = {
      val b = Map.newBuilder [K, V]
      iter foreach (x => b += (k (x) -> v (x)))
      b.result
    }}

  private [disk] def openFile (item: (Path, DiskDriveConfig), exec: ExecutorService) = {
    val (path, config) = item
    import StandardOpenOption.{CREATE, READ, WRITE}
    val file = File.open (path, exec, CREATE, READ, WRITE)
    (path, file, config)
  }

  private [disk] def reopenFile (path: Path, exec: ExecutorService) = {
    import StandardOpenOption.{READ, WRITE}
    (path, File.open (path, exec, READ, WRITE))
  }}
