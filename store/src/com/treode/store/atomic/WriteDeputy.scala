package com.treode.store.atomic

import scala.util.{Failure, Success}

import com.treode.async.{Async, Callback, Fiber}
import com.treode.async.implicits._
import com.treode.cluster.RequestDescriptor
import com.treode.disk.RecordDescriptor
import com.treode.store.{Bytes, TimeoutException, TxClock, TxId, TxStatus, WriteOp, log}
import com.treode.store.atomic.{WriteDeputy => WD}
import com.treode.store.locks.LockSet

import Async.{guard, supply, when}
import Callback.ignore
import WriteDirector.deliberate

private class WriteDeputy (xid: TxId, kit: AtomicKit) {
  import kit.{disk, paxos, scheduler, tables, writers}
  import kit.config.{closedLifetime, preparingTimeout}
  import kit.library.releaser

  type WriteCallback = Callback [WriteResponse]

  private val fiber = new Fiber
  private val epoch = releaser.join()
  private var state: State = new Open

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
      state = new Preparing (ct, ops, cb)

    def abort (cb: WriteCallback): Unit =
      state = new Aborting (None, cb)

    def commit (wt: TxClock, cb: WriteCallback): Unit =
      state = new Tardy (wt, cb)

    def checkpoint(): Async [Unit] =
      supply()

    override def toString = s"WriteDeputy.Open($xid)"
  }

  class Preparing (ct: TxClock, ops: Seq [WriteOp], cb: WriteCallback) extends State {

    tables.prepare (ct, ops) run {
      case Success (prep) => prepared (prep)
      case Failure (t) => failed (t)
    }

    private def _failed (t: Throwable) {
      if (state == Preparing.this) {
        log.exceptionPreparingWrite (t)
        state = new Deliberating (ops, Some (WriteResponse.Failed))
        cb.pass (WriteResponse.Failed)
        throw t
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
        cb.pass (WriteResponse.Prepared (ft))
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
        cb.pass (rsp)
      }}

    private def stale(): Unit = fiber.execute {
      if (state == Preparing.this) {
        state = new Deliberating (ops, Some (WriteResponse.Advance))
        cb.pass (WriteResponse.Advance)
      }}

    private def prepared (prep: PrepareResult) {
      import PrepareResult._
      prep match {
        case Prepared (ft, locks) => prepared (ft, locks)
        case Collided (ks) => collided (ks)
        case Stale => stale()
      }}

    def prepare (ct: TxClock, ops: Seq [WriteOp], cb: WriteCallback): Unit = ()

    def commit (wt: TxClock, cb: WriteCallback): Unit =
      state = new Committing (wt, ops, None, cb)

    def abort (cb: WriteCallback): Unit =
      state = new Aborting (None, cb)

    def checkpoint(): Async [Unit] =
      WD.preparing.record (xid, ops)

    override def toString = s"WriteDeputy.Preparing($xid, $ct)"
  }

  class Prepared (ops: Seq [WriteOp], ft: TxClock, locks: LockSet) extends State {

    timeout (Prepared.this)

    private def rsp = WriteResponse.Prepared (ft)

    def prepare (ct: TxClock, ops: Seq [WriteOp], cb: WriteCallback): Unit =
      cb.pass (rsp)

    def commit (wt: TxClock, cb: WriteCallback): Unit =
      state = new Committing (wt, ops, Some (locks), cb)

    def abort (cb: WriteCallback): Unit =
      state = new Aborting (Some (locks), cb)

    def checkpoint(): Async [Unit] =
      WD.preparing.record (xid, ops)

    override def toString = s"WriteDeputy.Prepared($xid)"
  }

  class Deliberating (ops: Seq [WriteOp], rsp: Option [WriteResponse]) extends State {

    timeout (Deliberating.this)

    def prepare (ct: TxClock, ops: Seq [WriteOp], cb: WriteCallback): Unit =
      rsp foreach (cb.pass (_))

    def commit (wt: TxClock, cb: WriteCallback): Unit =
      state = new Committing (wt, ops, None, cb)

    def abort (cb: WriteCallback): Unit =
      state = new Aborting (None, cb)

    def checkpoint(): Async [Unit] =
      WD.preparing.record (xid, ops)

    override def toString = s"WriteDeputy.Deliberating($xid)"
  }

  class Tardy (wt: TxClock, cb: WriteCallback) extends State {

    releaser.leave (epoch)
    scheduler.delay (closedLifetime) (writers.remove (xid, WriteDeputy.this))

    def prepare (ct: TxClock, ops: Seq [WriteOp], cb: WriteCallback): Unit =
      new Committing (wt, ops, None, cb)

    def commit (wt: TxClock, cb: WriteCallback) = ()

    def abort (cb: WriteCallback): Unit =
      throw new IllegalStateException

    def checkpoint(): Async [Unit] =
      supply()

    override def toString = s"WriteDeputy.Tardy($xid, $wt)"
  }

  class Committing (
      wt: TxClock,
      ops: Seq [WriteOp],
      locks: Option [LockSet],
      cb: WriteCallback) extends State {

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
        cb.pass (WriteResponse.Committed)
        locks foreach (_.release())
      }}

    private def failed (t: Throwable): Unit = fiber.execute {
      if (state == Committing.this) {
        state = new Panicked (this, t)
        locks foreach (_.release())
        cb.pass (WriteResponse.Failed)
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

    releaser.leave (epoch)
    scheduler.delay (closedLifetime) (writers.remove (xid, WriteDeputy.this))

    def prepare (ct: TxClock, ops: Seq [WriteOp], cb: WriteCallback): Unit =
      cb.pass (WriteResponse.Committed)

    def commit (wt: TxClock, cb: WriteCallback): Unit =
      cb.pass (WriteResponse.Committed)

    def abort (cb: WriteCallback): Unit =
      throw new IllegalStateException

    def checkpoint(): Async [Unit] =
      WD.committed.record (xid, gens, wt)

    override def toString = s"WriteDeputy.Committed($xid, $wt)"
  }

  class Aborting (locks: Option [LockSet], cb: WriteCallback) extends State {

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
        cb.pass (WriteResponse.Aborted)
      }}

    private def failed (t: Throwable): Unit = fiber.execute {
      if (state == Aborting.this) {
        state = new Panicked (this, t)
        cb.pass (WriteResponse.Failed)
        throw t
      }}

    def prepare (ct: TxClock, ops: Seq [WriteOp], cb: WriteCallback): Unit =
      cb.pass (WriteResponse.Aborted)

    def commit (wt: TxClock, cb: WriteCallback): Unit =
      throw new IllegalStateException

    def abort (cb: WriteCallback): Unit = ()

    def checkpoint(): Async [Unit] =
      WD.aborted.record (xid)

    override def toString = s"WriteDeputy.Aborting($xid)"
  }

  class Aborted extends State {

    releaser.leave (epoch)
    scheduler.delay (closedLifetime) (writers.remove (xid, WriteDeputy.this))

    def status = None

    def prepare (ct: TxClock, ops: Seq [WriteOp], cb: WriteCallback): Unit =
      cb.pass (WriteResponse.Aborted)

    def commit (wt: TxClock, cb: WriteCallback): Unit =
      throw new IllegalStateException

    def abort (cb: WriteCallback): Unit =
      cb.pass (WriteResponse.Aborted)

    def checkpoint(): Async [Unit] =
      WD.aborted.record (xid)

    override def toString = s"WriteDeputy.Aborted($xid)"
  }

  class Panicked (s: State, thrown: Throwable) extends State {

    releaser.leave (epoch)
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
    fiber.guard (state.checkpoint())

  def dispose(): Unit =
    releaser.leave (epoch)

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
