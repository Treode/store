package com.treode

import java.util.logging.{Level, Logger}
import com.treode.cluster.MailboxId

import Level.WARNING

package store {

  sealed abstract class WriteResult

  object WriteResult {
    case class Written (vt: TxClock) extends WriteResult
    case class Collided (ks: Seq [Int]) extends WriteResult
    case object Stale extends WriteResult
  }}

package object store {

  private [store] object log {

    val logger = Logger.getLogger ("com.treode.store")

    def exceptionPreparingWrite (e: Throwable): Unit =
      logger.log (WARNING, s"Exception preparing write", e)

    def catalogUpdateMissingDiffs (id: MailboxId): Unit =
      logger.log (WARNING, s"A catalog update was missing diffs: $id")
  }}
