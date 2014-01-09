package com.treode.store

import java.nio.file.Path
import com.treode.async.Callback

package disk2 {

  private case class Segment (num: Int, pos: Long, limit: Long)

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
  }}

package object disk2 {

  private [disk2] val SuperBlockBits = 14
  private [disk2] val SuperBlockBytes = 1 << SuperBlockBits
  private [disk2] val SuperBlockMask = SuperBlockBytes - 1
  private [disk2] val DiskLeadBytes = 1 << (SuperBlockBits + 1)
}
