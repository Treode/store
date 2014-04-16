package com.treode

import java.util.logging.{Level, Logger}
import com.treode.async.AsyncIterator
import com.treode.cluster.PortId

import Level.WARNING

package store {

  class DeputyException extends Exception

  trait Op {
    def table: TableId
    def key: Bytes
  }

  sealed abstract class WriteResult

  object WriteResult {
    case class Written (vt: TxClock) extends WriteResult
    case class Collided (ks: Seq [Int]) extends WriteResult
    case object Stale extends WriteResult
    case object Timeout extends WriteResult
  }}

package object store {

  type CellIterator = AsyncIterator [Cell]

  private [store] object log {

    val logger = Logger.getLogger ("com.treode.store")

    def exceptionPreparingWrite (e: Throwable): Unit =
      logger.log (WARNING, s"Exception preparing write", e)

    def catalogUpdateMissingDiffs (id: PortId): Unit =
      logger.log (WARNING, s"A catalog update was missing diffs: $id")
  }}
