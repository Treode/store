package com.treode.store

import java.nio.file.Path
import com.treode.async.Callback

package disk2 {

  private case class Segment (num: Int, pos: Long, limit: Long)

  class DiskFullException extends Exception {
    override def getMessage = "DiskFull."
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

  class PanickedException (t: Throwable) extends Exception (t) {
    override def getMessage = "Panicked."
  }

  class RecoveryCompletedException extends Exception {
    override def getMessage = "Recovery completed."
  }}

package object disk2 {

  val SuperBlockBits = 14
  val SuperBlockBytes = 1 << SuperBlockBits
  val SuperBlockMask = SuperBlockBytes - 1
  val DiskLeadBytes = 1 << (SuperBlockBits + 1)
}
