package com.treode.store.atomic

import com.treode.async.{Callback, Fiber}
import com.treode.cluster.{RequestDescriptor, RequestMediator}
import com.treode.store.{Bytes, TxClock, TxId, WriteOp, log}
import com.treode.store.locks.LockSet

import WriteDeputy.ArchiveStatus

private class WriteDeputy (xid: TxId, kit: AtomicKit) {
  import kit.{scheduler, store}
  import kit.writers.db

  type WriteMediator = RequestMediator [WriteResponse]

  val fiber = new Fiber (scheduler)
  var state: State = new Opening

  trait State {
    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp])
    def commit (mdtr: WriteMediator, wt: TxClock)
    def abort (mdtr: WriteMediator)
    def timeout()
    def shutdown()
  }

  class Opening extends State {

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp]) {
      val s = new Restoring
      s.prepare (mdtr, ct, ops)
      s.restore()
      state = s
    }

    def abort (mdtr: WriteMediator) {
      val s = new Restoring
      s.abort (mdtr)
      s.restore()
      state = s
    }

    def commit (mdtr: WriteMediator, wt: TxClock) {
      val s = new Restoring
      s.commit (mdtr, wt)
      s.restore()
      state = s
    }

    def timeout(): Unit =
      throw new IllegalStateException

    def shutdown(): Unit =
      throw new IllegalStateException

    override def toString = "Deputy.Opening"
  }

  class Restoring extends State {

    var _prepare = Option.empty [(WriteMediator, TxClock, Seq [WriteOp])]
    var _abort = Option.empty [WriteMediator]
    var _commit = Option.empty [(WriteMediator, TxClock)]

    def catchup() {
      _abort match {
        case Some (mdtr) => state.abort (mdtr)
        case None => ()
      }
      _commit match {
        case Some ((mdtr, wt)) => state.commit (mdtr, wt)
        case None => ()
      }
      _prepare match {
        case Some ((mdtr, ct, ops)) => state.prepare (mdtr, ct, ops)
        case None => ()
      }}

    def restore() {
      db.get (xid.id) run (new Callback [Option [Bytes]] {

        def pass (_status: Option [Bytes]): Unit = fiber.execute {
          val status = _status map (_.unpickle (ArchiveStatus.pickler))
          if (state == Restoring.this) {
            state = status match {
              case Some (ArchiveStatus.Aborted) =>
                new Aborted
              case Some (ArchiveStatus.Committed (wt)) =>
                new Committed
              case None =>
                new Open
            }}
          catchup()
        }

        def fail (t: Throwable): Unit = fiber.execute {
          if (state == Restoring.this) {
            state = new Panicked
            throw t
          }}})
    }

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp]): Unit =
      _prepare = Some ((mdtr, ct, ops))

    def abort (mdtr: WriteMediator): Unit =
      _abort = Some (mdtr)

    def commit (mdtr: WriteMediator, wt: TxClock): Unit =
      _commit = Some ((mdtr, wt))

    def timeout(): Unit =
      throw new IllegalStateException

    def shutdown(): Unit =
      state = new Shutdown

    override def toString = "Deputy.Restoring"
  }

  class Open extends State {

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp]): Unit =
      state = new Preparing (mdtr, ct, ops)

    def abort (mdtr: WriteMediator) {
      state = new Aborted
      mdtr.respond (WriteResponse.Aborted)
    }

    def commit (mdtr: WriteMediator, wt: TxClock): Unit =
      state = new Tardy (mdtr, wt)

    def timeout(): Unit =
      throw new IllegalStateException

    def shutdown(): Unit =
      state = new Shutdown

    override def toString = "Deputy.Open"
  }

  class Preparing (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp]) extends State {

    store.prepare (ct, ops) run (new Callback [PrepareResult] {

      def pass (result: PrepareResult): Unit = fiber.execute {

        result match {
          case PrepareResult.Prepared (ft, locks) =>
            if (state == Preparing.this) {
              state = new Prepared (ops, ft, locks)
              mdtr.respond (WriteResponse.Prepared (ft))
            } else {
              locks.release()
            }

          case PrepareResult.Collided (ks) =>
            if (state == Preparing.this) {
              val rsp = WriteResponse.Collisions (ks.toSet)
              state = new Deliberating (ops, rsp)
              mdtr.respond (rsp)
            }

          case PrepareResult.Stale =>
            if (state == Preparing.this) {
              state = new Deliberating (ops, WriteResponse.Advance)
              mdtr.respond (WriteResponse.Advance)
            }}}

      def fail (t: Throwable): Unit = fiber.execute {
        if (state == Preparing.this) {
          log.exceptionPreparingWrite (t)
          state = new Deliberating (ops, WriteResponse.Failed)
          mdtr.respond (WriteResponse.Failed)
        }}})

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp]): Unit = ()

    def commit (mdtr: WriteMediator, wt: TxClock): Unit =
      state = new Committing (mdtr, wt, ops, None)

    def abort (mdtr: WriteMediator) {
      state = new Aborted
      mdtr.respond (WriteResponse.Aborted)
    }

    def timeout(): Unit =
      throw new IllegalStateException

    def shutdown(): Unit =
      state = new Shutdown
  }

  class Prepared (ops: Seq [WriteOp], vt: TxClock, locks: LockSet) extends State {

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp]): Unit =
      mdtr.respond (WriteResponse.Prepared (vt))

    def commit (mdtr: WriteMediator, wt: TxClock): Unit =
      state = new Committing (mdtr, wt, ops, Some (locks))

    def abort (mdtr: WriteMediator) {
      state = new Aborted
      locks.release()
      mdtr.respond (WriteResponse.Aborted)
    }

    def timeout(): Unit =
      throw new IllegalStateException

    def shutdown() {
      state = new Shutdown
      locks.release()
    }}

  class Deliberating (ops: Seq [WriteOp], rsp: WriteResponse) extends State {

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp]): Unit =
      mdtr.respond (rsp)

    def commit (mdtr: WriteMediator, wt: TxClock): Unit =
      new Committing (mdtr, wt, ops, None)

    def abort (mdtr: WriteMediator): Unit =
      state = new Aborted

    def timeout() = ()

    def shutdown(): Unit =
      state = new Shutdown

    override def toString = "Deputy.Deliberating"
  }

  class Tardy (mdtr: WriteMediator, wt: TxClock) extends State {

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp]): Unit =
      new Committing (mdtr, wt, ops, None)

    def commit (mdtr: WriteMediator, wt: TxClock) = ()

    def abort (mdtr: WriteMediator): Unit =
      throw new IllegalStateException

    def timeout() = ()

    def shutdown(): Unit =
      state = new Shutdown

    override def toString = "Deputy.Tardy"
  }

  class Committing (
      mdtr: WriteMediator,
      wt: TxClock,
      ops: Seq [WriteOp],
      locks: Option [LockSet]) extends State {

    try {
      store.commit (wt, ops)
      state = new Committed
      mdtr.respond (WriteResponse.Committed)
    } catch {
      case t: Throwable =>
        state = new Panicked
        throw t
    } finally {
      locks foreach (_.release())
    }

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp]): Unit = ()

    def commit (mdtr: WriteMediator, wt: TxClock): Unit = ()

    def abort (mdtr: WriteMediator): Unit =
      throw new IllegalStateException

    def timeout() = ()

    def shutdown(): Unit =
      state = new Shutdown

    override def toString = "Deputy.Committing"
  }

  class Committed extends State {

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp]): Unit =
      mdtr.respond (WriteResponse.Committed)

    def commit (mdtr: WriteMediator, wt: TxClock): Unit =
      mdtr.respond (WriteResponse.Committed)

    def abort (mdtr: WriteMediator): Unit =
      throw new IllegalStateException

    def timeout() = ()

    def shutdown() =
      state = new Shutdown

    override def toString = "Deputy.Committed"
  }

  class Aborted extends State {

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp]): Unit =
      mdtr.respond (WriteResponse.Aborted)

    def commit (mdtr: WriteMediator, wt: TxClock): Unit =
      throw new IllegalStateException

    def abort (mdtr: WriteMediator): Unit =
      mdtr.respond (WriteResponse.Aborted)

    def timeout() = ()

    def shutdown(): Unit =
      state = new Shutdown

    override def toString = "Deputy.Aborted"
  }

  class Shutdown extends State {

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp]) = ()
    def commit (mdtr: WriteMediator, wt: TxClock) = ()
    def abort (mdtr: WriteMediator) = ()
    def timeout() = ()
    def shutdown() = ()

    override def toString = "Deputy.Shutdown"
  }

  class Panicked extends Shutdown {

    override def toString = "Deputy.Panicked"
  }

  def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp]): Unit =
    fiber.execute (state.prepare (mdtr, ct, ops))

  def commit (mdtr: WriteMediator, wt: TxClock): Unit =
    fiber.execute (state.commit (mdtr, wt))

  def abort (mdtr: WriteMediator): Unit =
    fiber.execute (state.abort (mdtr))

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

  sealed abstract class ActiveStatus

  object ActiveStatus {

    case class Closed (xid: TxId, wt: TxClock) extends ActiveStatus

    val pickler = {
      import AtomicPicklers._
      tagged [ActiveStatus] (
          0x1 -> wrap (txId, txClock)
                 .build ((Closed.apply _).tupled)
                 .inspect (v => (v.xid, v.wt)))
    }}

  sealed abstract class ArchiveStatus

  object ArchiveStatus {

    case object Aborted extends ArchiveStatus

    case class Committed (wt: TxClock) extends ArchiveStatus

    val pickler = {
      import AtomicPicklers._
      tagged [ArchiveStatus] (
          0x1 -> const (Aborted),
          0x2 -> wrap (txClock) .build (Committed.apply _) .inspect (_.wt))
    }}}
