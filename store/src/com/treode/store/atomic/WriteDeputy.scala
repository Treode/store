package com.treode.store.atomic

import scala.util.{Failure, Success}

import com.treode.async.{Async, Callback, Fiber}
import com.treode.cluster.{RequestDescriptor, RequestMediator}
import com.treode.disk.RecordDescriptor
import com.treode.store.{Bytes, TxClock, TxId, TxStatus, WriteOp, log}
import com.treode.store.atomic.{WriteDeputy => WD}
import com.treode.store.locks.LockSet

import Async.{guard, supply, when}
import WriteDirector.deliberate

private class WriteDeputy (xid: TxId, kit: AtomicKit) {
  import kit.{disks, paxos, scheduler, tables, writers}
  import kit.config.{closedLifetime, preparingTimeout}

  type WriteMediator = RequestMediator [WriteResponse]
  object WriteMediator {
    val void = RequestMediator.void [WriteResponse]
  }

  val fiber = new Fiber
  var state: State = new Open

  trait State {
    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp])
    def commit (mdtr: WriteMediator, wt: TxClock)
    def abort (mdtr: WriteMediator)
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
            WriteDeputy.this.abort (WriteMediator.void)
          case Success (TxStatus.Committed (wt)) =>
            WriteDeputy.this.commit (WriteMediator.void, wt)
          case Failure (t) =>
            panic (s, t)
        }}

  class Open extends State {

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp]): Unit =
      state = new Preparing (mdtr, ct, ops)

    def abort (mdtr: WriteMediator): Unit =
      state = new Aborting (mdtr, None)

    def commit (mdtr: WriteMediator, wt: TxClock): Unit =
      state = new Tardy (mdtr, wt)

    def checkpoint(): Async [Unit] =
      supply()

    override def toString = "Deputy.Open"
  }

  class Preparing (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp]) extends State {

    tables.prepare (ct, ops) run {
      case Success (prep) => prepared (prep)
      case Failure (t) => failed (t)
    }

    private def _failed (t: Throwable) {
      if (state == Preparing.this) {
        log.exceptionPreparingWrite (t)
        state = new Deliberating (ops, Some (WriteResponse.Failed))
        mdtr.respond (WriteResponse.Failed)
      }}

    private def failed (t: Throwable): Unit = fiber.execute {
      _failed (t)
    }

    private def failed (locks: LockSet, t: Throwable): Unit = fiber.execute {
      locks.release()
      _failed (t)
    }

    private def logged (ft: TxClock, locks: LockSet): Unit = fiber.execute {
      if (state == Preparing.this) {
        state = new Prepared (ops, ft, locks)
        mdtr.respond (WriteResponse.Prepared (ft))
      } else {
        locks.release()
      }}

    private def prepared (ft: TxClock, locks: LockSet): Unit = fiber.execute {
      if (state == Preparing.this) {
        WD.preparing.record (xid, ops) run {
          case Success (v) => logged (ft, locks)
          case Failure (t) => failed (locks, t)
        }
      } else {
        locks.release()
      }}

    private def collided (ks: Seq [Int]): Unit = fiber.execute {
      if (state == Preparing.this) {
        val rsp = WriteResponse.Collisions (ks.toSet)
        state = new Deliberating (ops, Some (rsp))
        mdtr.respond (rsp)
      }}

    private def stale(): Unit = fiber.execute {
      if (state == Preparing.this) {
        state = new Deliberating (ops, Some (WriteResponse.Advance))
        mdtr.respond (WriteResponse.Advance)
      }}

    private def prepared (prep: PrepareResult) {
      import PrepareResult._
      prep match {
        case Prepared (ft, locks) => prepared (ft, locks)
        case Collided (ks) => collided (ks)
        case Stale => stale()
      }}

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp]): Unit = ()

    def commit (mdtr: WriteMediator, wt: TxClock): Unit =
      state = new Committing (mdtr, wt, ops, None)

    def abort (mdtr: WriteMediator): Unit =
      state = new Aborting (mdtr, None)

    def checkpoint(): Async [Unit] =
      WD.preparing.record (xid, ops)

  }

  class Prepared (ops: Seq [WriteOp], ft: TxClock, locks: LockSet) extends State {

    timeout (Prepared.this)

    private def rsp = WriteResponse.Prepared (ft)

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp]): Unit =
      mdtr.respond (rsp)

    def commit (mdtr: WriteMediator, wt: TxClock): Unit =
      state = new Committing (mdtr, wt, ops, Some (locks))

    def abort (mdtr: WriteMediator): Unit =
      state = new Aborting (mdtr, Some (locks))

    def checkpoint(): Async [Unit] =
      WD.preparing.record (xid, ops)
  }

  class Deliberating (ops: Seq [WriteOp], rsp: Option [WriteResponse]) extends State {

    timeout (Deliberating.this)

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp]): Unit =
      rsp foreach (mdtr.respond _)

    def commit (mdtr: WriteMediator, wt: TxClock): Unit =
      new Committing (mdtr, wt, ops, None)

    def abort (mdtr: WriteMediator): Unit =
      state = new Aborting (mdtr, None)

    def checkpoint(): Async [Unit] =
      WD.preparing.record (xid, ops)

    override def toString = "Deputy.Deliberating"
  }

  class Tardy (mdtr: WriteMediator, wt: TxClock) extends State {

    timeout (Tardy.this)

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp]): Unit =
      new Committing (mdtr, wt, ops, None)

    def commit (mdtr: WriteMediator, wt: TxClock) = ()

    def abort (mdtr: WriteMediator): Unit =
      throw new IllegalStateException

    def checkpoint(): Async [Unit] =
      supply()

    override def toString = "Deputy.Tardy"
  }

  class Committing (
      mdtr: WriteMediator,
      wt: TxClock,
      ops: Seq [WriteOp],
      locks: Option [LockSet]) extends State {

    val gens = tables.commit (wt, ops)
    guard {
      for {
        _ <- when (locks.isEmpty) (WD.preparing.record (xid, ops))
        _ <- WD.committed.record (xid, gens, wt)
      } yield ()
    } run {
      case Success (v) => logged()
      case Failure (t) => failed (t)
    }

    private def logged(): Unit = fiber.execute {
      if (state == Committing.this) {
        state = new Committed (gens, wt)
        mdtr.respond (WriteResponse.Committed)
        locks foreach (_.release())
      }}

    private def failed (t: Throwable): Unit = fiber.execute {
      if (state == Committing.this) {
        state = new Panicked (this, t)
        mdtr.respond (WriteResponse.Failed)
        locks foreach (_.release())
        throw t
      }}

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp]): Unit = ()

    def commit (mdtr: WriteMediator, wt: TxClock): Unit = ()

    def abort (mdtr: WriteMediator): Unit =
      throw new IllegalStateException

    def checkpoint(): Async [Unit] =
      WD.committed.record (xid, gens, wt)

    override def toString = "Deputy.Committing"
  }

  class Committed (gens: Seq [Long], wt: TxClock) extends State {

    fiber.delay (closedLifetime) (writers.remove (xid, WriteDeputy.this))

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp]): Unit =
      mdtr.respond (WriteResponse.Committed)

    def commit (mdtr: WriteMediator, wt: TxClock): Unit =
      mdtr.respond (WriteResponse.Committed)

    def abort (mdtr: WriteMediator): Unit =
      throw new IllegalStateException

    def checkpoint(): Async [Unit] =
      WD.committed.record (xid, gens, wt)

    override def toString = "Deputy.Committed"
  }

  class Aborting (mdtr: WriteMediator, locks: Option [LockSet]) extends State {

    guard {
      locks foreach (_.release())
      WD.aborted.record (xid)
    } run {
      case Success (v) => logged()
      case Failure (t) => failed (t)
    }

    private def logged(): Unit = fiber.execute {
      if (state == Aborting.this) {
        state = new Aborted
        mdtr.respond (WriteResponse.Aborted)
      }}

    private def failed (t: Throwable): Unit = fiber.execute {
      if (state == Aborting.this) {
        state = new Panicked (this, t)
        mdtr.respond (WriteResponse.Failed)
        throw t
      }}

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp]): Unit =
      mdtr.respond (WriteResponse.Aborted)

    def commit (mdtr: WriteMediator, wt: TxClock): Unit =
      throw new IllegalStateException

    def abort (mdtr: WriteMediator): Unit = ()

    def checkpoint(): Async [Unit] =
      WD.aborted.record (xid)

    override def toString = "Deputy.Aborting"
  }

  class Aborted extends State {

    fiber.delay (closedLifetime) (writers.remove (xid, WriteDeputy.this))

    def status = None

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp]): Unit =
      mdtr.respond (WriteResponse.Aborted)

    def commit (mdtr: WriteMediator, wt: TxClock): Unit =
      throw new IllegalStateException

    def abort (mdtr: WriteMediator): Unit =
      mdtr.respond (WriteResponse.Aborted)

    def checkpoint(): Async [Unit] =
      WD.aborted.record (xid)

    override def toString = "Deputy.Aborted"
  }

  class Panicked (s: State, thrown: Throwable) extends State {

    fiber.delay (closedLifetime) (writers.remove (xid, WriteDeputy.this))

    def checkpoint(): Async [Unit] =
      s.checkpoint()

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp]) = ()
    def commit (mdtr: WriteMediator, wt: TxClock) = ()
    def abort (mdtr: WriteMediator) = ()

    override def toString = s"Deputy.Panicked ($thrown)"
  }

  def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp]): Unit =
    fiber.execute (state.prepare (mdtr, ct, ops))

  def commit (mdtr: WriteMediator, wt: TxClock): Unit =
    fiber.execute (state.commit (mdtr, wt))

  def abort (mdtr: WriteMediator): Unit =
    fiber.execute (state.abort (mdtr))

  def checkpoint(): Async [Unit] =
    fiber.guard (state.checkpoint())

  override def toString = state.toString
}

private object WriteDeputy {

  val prepare = {
    import AtomicPicklers._
    RequestDescriptor (
        0xFFDD52697F320AD1L,
        tuple (txId, txClock, seq (writeOp)),
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
    RecordDescriptor (0x875B728C8F37467AL, tuple (txId, seq (writeOp)))
  }

  val committed = {
    import AtomicPicklers._
    RecordDescriptor (0x5A5C7DA53F8C60F6L, tuple (txId, seq (ulong), txClock))
  }

  val aborted = {
    import AtomicPicklers._
    RecordDescriptor (0xF83F939483B72F77L, txId)
  }}
