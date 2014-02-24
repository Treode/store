package com.treode

import java.io.Closeable
import java.util.logging.{Level, Logger}

import com.treode.async.{Async, Callback}
import com.treode.pickle.Pickler
import com.treode.store.locks.LockSet

import Level.WARNING

package store {

  trait ReadCallback extends Callback [Seq [Value]]

  sealed abstract class WriteResult

  object WriteResult {
    case class Written (vt: TxClock) extends WriteResult
    case class Collided (ks: Seq [Int]) extends WriteResult
    case object Stale extends WriteResult
  }

  trait Store {
    def read (rt: TxClock, ops: Seq [ReadOp]): Async [Seq [Value]]
    def write (xid: TxId, ct: TxClock, ops: Seq [WriteOp]): Async [WriteResult]
  }

  private sealed abstract class PrepareResult

  private object PrepareResult {
    case class Prepared (vt: TxClock, locks: LockSet) extends PrepareResult
    case class Collided (ks: Seq [Int]) extends PrepareResult
    case object Stale extends PrepareResult
  }

  private trait PrepareCallback extends Callback [Preparation] {
    def collisions (ks: Set [Int])
    def advance()
  }

  private trait PreparableStore {
    def read (rt: TxClock, ops: Seq [ReadOp]): Async [Seq [Value]]
    def prepare (ct: TxClock, ops: Seq [WriteOp]): Async [PrepareResult]
    def commit (wt: TxClock, ops: Seq [WriteOp]): Async [Unit]
  }

  private trait LocalStore extends PreparableStore with Closeable
}

package object store {

  private [store] object log {

    val logger = Logger.getLogger ("com.treode.store")

    def exceptionPreparingWrite (e: Throwable): Unit =
      logger.log (WARNING, s"Exception preparing write", e)
  }}
