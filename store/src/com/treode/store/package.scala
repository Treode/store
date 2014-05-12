package com.treode

import java.util.concurrent.{TimeoutException => JTimeoutException}
import java.util.logging.{Level, Logger}

import com.treode.async.AsyncIterator
import com.treode.cluster.{RemoteException => CRemoteException, PortId}

import Level.WARNING

package store {

  class CollisionException (val indexes: Seq [Int]) extends Exception

  class StaleException extends Exception

  class TimeoutException extends JTimeoutException

  class RemoteException extends CRemoteException

  trait Op {
    def table: TableId
    def key: Bytes
  }}

package object store {

  type CellIterator = AsyncIterator [Cell]

  private [store] implicit class RichCellIterator (iter: CellIterator) {

    def dedupe: CellIterator =
      Filters.dedupe (iter)

    def retire (limit: TxClock): CellIterator =
      Filters.retire (iter, limit)
  }

  private [store] object log {

    val logger = Logger.getLogger ("com.treode.store")

    def exceptionPreparingWrite (e: Throwable): Unit =
      logger.log (WARNING, s"Exception preparing write", e)

    def catalogUpdateMissingDiffs (id: PortId): Unit =
      logger.log (WARNING, s"A catalog update was missing diffs: $id")
  }}
