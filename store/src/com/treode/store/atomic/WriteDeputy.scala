/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.treode.store.atomic

import scala.util.{Failure, Success, Try}

import com.treode.async.{Async, Callback, Fiber}
import com.treode.async.implicits._
import com.treode.cluster.RequestDescriptor
import com.treode.disk.RecordDescriptor
import com.treode.store.{Bytes, TimeoutException, TxClock, TxId, TxStatus, WriteOp, log}
import com.treode.store.atomic.{WriteDeputy => WD, WriteResponse => WR}
import com.treode.store.locks.LockSet

import Async.{guard, supply, when}
import Callback.ignore
import WriteDirector.deliberate

private class WriteDeputy (xid: TxId, kit: AtomicKit) {
  import kit.{disk, paxos, scheduler, tstore, writers}
  import kit.config.{closedLifetime, preparingTimeout}
  import kit.library.releaser

  type WriteCallback = Callback [WriteResponse]

  val fiber = new Fiber
  var state: State = new Open

  trait State {
    def prepare (ct: TxClock, ops: Seq [WriteOp], cb: WriteCallback)
    def commit (wt: TxClock, cb: WriteCallback)
    def abort (cb: WriteCallback)
    def checkpoint(): Async [Unit]
  }

  private def panic (s: State, t: Throwable): Unit =
    fiber.execute {
      if (state == s) {
        state = new Panicked (state, t)
        throw t
      }}

  private def timeout (s: State): Unit =
    fiber.delay (preparingTimeout) {
      if (state == s)
        deliberate.propose (xid.id, xid.time, TxStatus.Aborted) .run {
          case Success (TxStatus.Aborted) =>
            WriteDeputy.this.abort() run (ignore)
          case Success (TxStatus.Committed (wt)) =>
            WriteDeputy.this.commit (wt) run (ignore)
          case Failure (_: TimeoutException) =>
            timeout (s)
          case Failure (t) =>
            panic (s, t)
        }}

  class Open extends State {

    def prepare (ct: TxClock, ops: Seq [WriteOp], cb: WriteCallback): Unit =
      state = new Preparing (ct, ops, releaser.join(), cb)

    def abort (cb: WriteCallback): Unit =
      state = new Aborting (None, None, cb)

    def commit (wt: TxClock, cb: WriteCallback): Unit =
      state = new Tardy (wt, cb)

    def checkpoint(): Async [Unit] =
      supply (())

    override def toString = s"WriteDeputy.Open($xid)"
  }

  class Preparing (ct: TxClock, ops: Seq [WriteOp], epoch: Int, cb: WriteCallback) extends State {

    private def prepared (result: Try [PrepareResult]): Unit = fiber.execute {
      import PrepareResult._
      if (state == Preparing.this) {
        result match {
          case Success (Prepared (ft, locks)) =>
            state = new Recording (ct, ft, ops, epoch, locks, cb)
          case Success (Collided (ks)) =>
            val rsp = WR.Collisions (ks.toSet)
            state = new Deliberating (ct, ops, epoch, Some (rsp))
            cb.pass (rsp)
          case Success (Stale) =>
            state = new Deliberating (ct, ops, epoch, Some (WR.Advance))
            cb.pass (WR.Advance)
          case Failure (t) =>
            state = new Deliberating (ct, ops, epoch, Some (WR.Failed))
            log.exceptionPreparingWrite (t)
            cb.pass (WR.Failed)
        }
      } else {
        result match {
          case Success (Prepared (ft, locks)) =>
            locks.release()
          case _ =>
            ()
        }}}

    tstore.prepare (ct, ops) run (prepared)

    def prepare (ct: TxClock, ops: Seq [WriteOp], cb: WriteCallback): Unit = ()

    def commit (wt: TxClock, cb: WriteCallback): Unit =
      state = new Committing (ct, wt, ops, Some (epoch), None, cb)

    def abort (cb: WriteCallback): Unit =
      state = new Aborting (Some (epoch), None, cb)

    def checkpoint(): Async [Unit] =
      WD.preparing.record (xid, ct, ops)

    override def toString = s"WriteDeputy.Preparing($xid, $ct)"
  }

  class Recording (
      ct: TxClock,
      ft: TxClock,
      ops: Seq [WriteOp],
      epoch: Int,
      locks: LockSet,
      cb: Callback [WriteResponse]
  ) extends State {

    private def recorded (result: Try [Unit]): Unit = fiber.execute {
      if (state == Recording.this) {
        result match {
          case Success (v) =>
            state = new Prepared (ct, ft, ops, epoch, locks)
            cb.pass (WR.Prepared (ft))
          case Failure (t) =>
            state = new Deliberating (ct, ops, epoch, Some (WR.Failed))
            log.exceptionPreparingWrite (t)
            locks.release()
            cb.pass (WR.Failed)
            throw t
        }}}

    WD.prepared.record (xid, ct, ops) run (recorded _)

    def prepare (ct: TxClock, ops: Seq [WriteOp], cb: WriteCallback): Unit = ()

    def commit (wt: TxClock, cb: WriteCallback): Unit =
      state = new Committing (ct, wt, ops, Some (epoch), Some (locks), cb)

    def abort (cb: WriteCallback): Unit =
      state = new Aborting (Some (epoch), Some (locks), cb)

    def checkpoint(): Async [Unit] =
      WD.prepared.record (xid, ct, ops)

    override def toString = s"WriteDeputy.Recording($xid)"
  }

  class Prepared (
      ct: TxClock,
      ft: TxClock,
      ops: Seq [WriteOp],
      epoch: Int,
      locks: LockSet
  ) extends State {

    timeout (Prepared.this)

    def prepare (ct: TxClock, ops: Seq [WriteOp], cb: WriteCallback): Unit =
      cb.pass (WR.Prepared (ft))

    def commit (wt: TxClock, cb: WriteCallback): Unit =
      state = new Committing (ct, wt, ops, Some (epoch), Some (locks), cb)

    def abort (cb: WriteCallback): Unit =
      state = new Aborting (Some (epoch), Some (locks), cb)

    def checkpoint(): Async [Unit] =
      WD.prepared.record (xid, ct, ops)

    override def toString = s"WriteDeputy.Prepared($xid)"
  }

  class Deliberating (
      ct: TxClock,
      ops: Seq [WriteOp],
      epoch: Int,
      rsp: Option [WriteResponse]
  ) extends State {

    timeout (Deliberating.this)

    def prepare (ct: TxClock, ops: Seq [WriteOp], cb: WriteCallback): Unit =
      rsp foreach (cb.pass (_))

    def commit (wt: TxClock, cb: WriteCallback): Unit =
      state = new Committing (ct, wt, ops, Some (epoch), None, cb)

    def abort (cb: WriteCallback): Unit =
      state = new Aborting (Some (epoch), None, cb)

    def checkpoint(): Async [Unit] =
      WD.preparing.record (xid, ct, ops)

    override def toString = s"WriteDeputy.Deliberating($xid)"
  }

  class Tardy (wt: TxClock, cb: WriteCallback) extends State {

    scheduler.delay (closedLifetime) (writers.remove (xid, WriteDeputy.this))

    def prepare (ct: TxClock, ops: Seq [WriteOp], cb: WriteCallback): Unit =
      state = new Committing (ct, wt, ops, None, None, cb)

    def commit (wt: TxClock, cb: WriteCallback) = ()

    def abort (cb: WriteCallback): Unit =
      throw new IllegalStateException

    def checkpoint(): Async [Unit] =
      supply (())

    override def toString = s"WriteDeputy.Tardy($xid, $wt)"
  }

  class Committing (
      ct: TxClock,
      wt: TxClock,
      ops: Seq [WriteOp],
      epoch: Option [Int],
      locks: Option [LockSet],
      cb: WriteCallback) extends State {

    val gens = tstore.commit (wt, ops)
    guard {
      for {
        _ <- when (locks.isEmpty) (WD.prepared.record (xid, ct, ops))
        _ <- WD.committed.record (xid, gens, wt)
      } yield ()
    } run {
      case Success (v) => logged()
      case Failure (t) => failed (t)
    }

    private def logged(): Unit = fiber.execute {
      if (state == Committing.this) {
        state = new Committed (gens, wt)
        epoch foreach (releaser.leave (_))
        locks foreach (_.release())
        cb.pass (WR.Committed)
      }}

    private def failed (t: Throwable): Unit = fiber.execute {
      if (state == Committing.this) {
        state = new Panicked (this, t)
        epoch foreach (releaser.leave (_))
        locks foreach (_.release())
        cb.pass (WR.Failed)
        throw t
      }}

    def prepare (ct: TxClock, ops: Seq [WriteOp], cb: WriteCallback): Unit = ()

    def commit (wt: TxClock, cb: WriteCallback): Unit = ()

    def abort (cb: WriteCallback): Unit =
      throw new IllegalStateException

    def checkpoint(): Async [Unit] =
      WD.committed.record (xid, gens, wt)

    override def toString = s"Deputy.Committing($xid)"
  }

  class Committed (gens: Seq [Long], wt: TxClock) extends State {

    scheduler.delay (closedLifetime) (writers.remove (xid, WriteDeputy.this))

    def prepare (ct: TxClock, ops: Seq [WriteOp], cb: WriteCallback): Unit =
      cb.pass (WR.Committed)

    def commit (wt: TxClock, cb: WriteCallback): Unit =
      cb.pass (WR.Committed)

    def abort (cb: WriteCallback): Unit =
      throw new IllegalStateException

    def checkpoint(): Async [Unit] =
      WD.committed.record (xid, gens, wt)

    override def toString = s"WriteDeputy.Committed($xid, $wt)"
  }

  class Aborting (
      epoch: Option [Int],
      locks: Option [LockSet],
      cb: WriteCallback
  ) extends State {

    guard {
      WD.aborted.record (xid)
    } run {
      case Success (v) => logged()
      case Failure (t) => failed (t)
    }

    private def logged(): Unit = fiber.execute {
      if (state == Aborting.this) {
        state = new Aborted
        epoch foreach (releaser.leave (_))
        locks foreach (_.release())
        cb.pass (WR.Aborted)
      }}

    private def failed (t: Throwable): Unit = fiber.execute {
      if (state == Aborting.this) {
        state = new Panicked (this, t)
        epoch foreach (releaser.leave (_))
        locks foreach (_.release())
        cb.pass (WR.Failed)
        throw t
      }}

    def prepare (ct: TxClock, ops: Seq [WriteOp], cb: WriteCallback): Unit = ()

    def commit (wt: TxClock, cb: WriteCallback): Unit =
      throw new IllegalStateException

    def abort (cb: WriteCallback): Unit = ()

    def checkpoint(): Async [Unit] =
      WD.aborted.record (xid)

    override def toString = s"WriteDeputy.Aborting($xid)"
  }

  class Aborted extends State {

    scheduler.delay (closedLifetime) (writers.remove (xid, WriteDeputy.this))

    def status = None

    def prepare (ct: TxClock, ops: Seq [WriteOp], cb: WriteCallback): Unit =
      cb.pass (WR.Aborted)

    def commit (wt: TxClock, cb: WriteCallback): Unit =
      throw new IllegalStateException

    def abort (cb: WriteCallback): Unit =
      cb.pass (WR.Aborted)

    def checkpoint(): Async [Unit] =
      WD.aborted.record (xid)

    override def toString = s"WriteDeputy.Aborted($xid)"
  }

  class Panicked (s: State, thrown: Throwable) extends State {

    scheduler.delay (closedLifetime) (writers.remove (xid, WriteDeputy.this))

    def checkpoint(): Async [Unit] =
      s.checkpoint()

    def prepare (ct: TxClock, ops: Seq [WriteOp], cb: WriteCallback) = ()
    def commit (wt: TxClock, cb: WriteCallback) = ()
    def abort (cb: WriteCallback) = ()

    override def toString = s"WriteDeputy.Panicked($xid, $thrown)"
  }

  def prepare (ct: TxClock, ops: Seq [WriteOp]): Async [WriteResponse] =
    fiber.async (state.prepare (ct, ops, _))

  def commit (wt: TxClock): Async [WriteResponse] =
    fiber.async (state.commit (wt, _))

  def abort(): Async [WriteResponse] =
    fiber.async (state.abort (_))

  def checkpoint(): Async [Unit] =
    fiber.async (cb => state.checkpoint() run (cb))

  override def toString = state.toString
}

private object WriteDeputy {

  val prepare = {
    import AtomicPicklers._
    RequestDescriptor (
        0xFFDD52697F320AD1L,
        tuple (uint, txId, txClock, seq (writeOp)),
        writeResponse)
  }

  val commit = {
    import AtomicPicklers._
    RequestDescriptor (0xFFF9E8BCFABDFFE6L, tuple (txId, txClock), writeResponse)
  }

  val abort = {
    import AtomicPicklers._
    RequestDescriptor (0xFF2D9D46D1F3A7F9L, txId, writeResponse)
  }

  val preparing = {
    import AtomicPicklers._
    RecordDescriptor (0x12A4690B2129333DL, tuple (txId, txClock, seq (writeOp)))
  }

  val preparedV0 = {
    import AtomicPicklers._
    RecordDescriptor (0x875B728C8F37467AL, tuple (txId, seq (writeOp)))
  }

  val prepared = {
    import AtomicPicklers._
    RecordDescriptor (0x9244FD7C53699533L, tuple (txId, txClock, seq (writeOp)))
  }

  val committed = {
    import AtomicPicklers._
    RecordDescriptor (0x5A5C7DA53F8C60F6L, tuple (txId, seq (ulong), txClock))
  }

  val aborted = {
    import AtomicPicklers._
    RecordDescriptor (0xF83F939483B72F77L, txId)
  }}
