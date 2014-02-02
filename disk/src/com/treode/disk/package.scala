package com.treode

import java.nio.file.Path
import com.treode.async.AsyncIterator

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

  class ReattachmentPendingException extends Exception {
    override def getMessage = "Reattachment pending."
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

  class RecoveryCompletedException extends Exception {
    override def getMessage = "Recovery completed."
  }

  private case class SegmentBounds (num: Int, pos: Long, limit: Long)

  private case class SegmentPointer (disk: Int, num: Int)
}

package object disk {

  private [disk] type ReplayIterator = AsyncIterator [(Long, Unit => Any)]

  private [disk] val SuperBlockBits = 14
  private [disk] val SuperBlockBytes = 1 << SuperBlockBits
  private [disk] val SuperBlockMask = SuperBlockBytes - 1
  private [disk] val DiskLeadBytes = 1 << (SuperBlockBits + 1)
  private [disk] val LogSegmentTrailerBytes = 20
}
