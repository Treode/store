package com.treode.store.cluster.atomic

import com.treode.cluster.{Host, MessageDescriptor, RequestDescriptor, RequestMediator}
import com.treode.concurrent.{Callback, Fiber}
import com.treode.store.{PrepareCallback, Transaction, TxClock, TxId, WriteOp}

private class Deputy (xid: TxId, kit: AtomicKit) {
  import Deputy._
  import kit.Deputies.{mainDb, openDb}
  import kit.store
  import kit.host.scheduler

  type AtomicMediator = RequestMediator [AtomicResponse]

  val fiber = new Fiber (scheduler)
  var state: State = new Restoring

  trait State {
    def prepare (mdtr: AtomicMediator, ct: TxClock, ops: Seq [WriteOp], cb: Callback [Unit])
    def commit (mdtr: AtomicMediator, wt: TxClock, cb: Callback [Unit])
    def abort (mdtr: AtomicMediator, cb: Callback [Unit])
    def timeout()
    def shutdown()
  }

  def _prepare (mdtr: AtomicMediator, ct: TxClock, ops: Seq [WriteOp], cb: Callback [Unit]): Unit =
    Callback.guard (cb) {
      store.prepare (ct, ops, new PrepareCallback {

        def pass (tx: Transaction) {
          state = Prepared.save (ops, tx)
          mdtr.respond (AtomicResponse.Prepared (tx.ft))
          cb()
        }

        def fail (t: Throwable) {
          state = new Aborting (ops, AtomicResponse.Failed)
          mdtr.respond (AtomicResponse.Failed)
          cb()
        }

        def collisions (ks: Set [Int]) {
          val rsp = AtomicResponse.Collisions (ks)
          state = new Aborting (ops, rsp)
          mdtr.respond (rsp)
          cb()
        }

        def advance() {
          state = new Aborting (ops, AtomicResponse.Advance)
          mdtr.respond (AtomicResponse.Advance)
          cb()
        }})
    }

  def _prepared (mdtr: AtomicMediator, rsp: AtomicResponse, cb: Callback [Unit]): Unit =
    Callback.guard (cb) {
      mdtr.respond (rsp)
      cb()
    }

  def _commit (mdtr: AtomicMediator, wt: TxClock, ops: Seq [WriteOp], cb: Callback [Unit]): Unit =
    Callback.guard (cb) {
      store.commit (wt, ops, new Callback [Unit] {

        def pass (v: Unit) {
          state = Committed.save()
          mdtr.respond (AtomicResponse.Committed)
          cb()
        }

        def fail (t: Throwable) = cb.fail (t)
      })
    }

  def _commit (mdtr: AtomicMediator, tx: Transaction, wt: TxClock, cb: Callback [Unit]): Unit =
    Callback.guard (cb) {
      tx.commit (wt, new Callback [Unit] {

        def pass (v: Unit) {
          state = Committed.save()
          mdtr.respond (AtomicResponse.Committed)
          cb()
        }

        def fail (t: Throwable) = cb.fail (t)
      })
    }

  def _committed (mdtr: AtomicMediator, cb: Callback [Unit]): Unit =
    Callback.guard (cb) {
      mdtr.respond (AtomicResponse.Committed)
      cb()
    }

  def _abort (mdtr: AtomicMediator, cb: Callback [Unit]): Unit =
    Callback.guard (cb) {
      state = Aborted.save()
      mdtr.respond (AtomicResponse.Aborted)
      cb()
    }

  def _abort (mdtr: AtomicMediator, tx: Transaction, cb: Callback [Unit]): Unit =
    Callback.guard (cb) {
      tx.abort()
      state = Aborted.save()
      mdtr.respond (AtomicResponse.Aborted)
      cb()
    }

  def _aborted (mdtr: AtomicMediator, cb: Callback [Unit]): Unit =
    Callback.guard (cb) {
      mdtr.respond (AtomicResponse.Aborted)
      cb()
    }

  class Restoring extends State {

    def restore (cb: Callback [Unit]) (f: State => Unit) {
      Callback.guard (cb) {
        mainDb.get (xid.id, Callback.unary {
          case Some (DeputyStatus.Prepared (ft, ops)) =>
            state = new Aborting (ops, AtomicResponse.Prepared (ft))
            f (state)
          case Some (DeputyStatus.Committed) =>
            state = Committed.forget()
            f (state)
          case Some (DeputyStatus.Aborted) =>
            state = Aborted.forget()
            f (state)
          case None =>
            state = new Open
            f (state)
        })
      }}

    def prepare (mdtr: AtomicMediator, ct: TxClock, ops: Seq [WriteOp], cb: Callback [Unit]): Unit =
      restore (cb) (_.prepare (mdtr, ct, ops, cb))

    def abort (mdtr: AtomicMediator, cb: Callback [Unit]): Unit =
      restore (cb) (_.abort (mdtr, cb))

    def commit (mdtr: AtomicMediator, wxt: TxClock, cb: Callback [Unit]): Unit =
      restore (cb) (_.commit (mdtr, wxt, cb))

    def timeout(): Unit =
      throw new IllegalStateException

    def shutdown(): Unit =
      throw new IllegalStateException

    override def toString = "Deputy.Restoring"
  }

  class Open extends State {

    def prepare (mdtr: AtomicMediator, ct: TxClock, ops: Seq [WriteOp], cb: Callback [Unit]): Unit =
      _prepare (mdtr, ct, ops, cb)

    def abort (mdtr: AtomicMediator, cb: Callback [Unit]): Unit =
      _abort (mdtr, cb)

    def commit (mdtr: AtomicMediator, wt: TxClock, cb: Callback [Unit]): Unit =
      Callback.guard (cb) {
        state = new Committing (wt)
        cb()
      }

    def timeout(): Unit =
      throw new IllegalStateException

    def shutdown(): Unit =
      state = new Shutdown

    override def toString = "Deputy.Open"
  }

  class Prepared private (tx: Transaction) extends State {

    def prepare (mdtr: AtomicMediator, ct: TxClock, ops: Seq [WriteOp], cb: Callback [Unit]): Unit =
      _prepared (mdtr, AtomicResponse.Prepared (tx.ft), cb)

    def commit (mdtr: AtomicMediator, wt: TxClock, cb: Callback [Unit]): Unit =
      _commit (mdtr, tx, wt, cb)

    def abort (mdtr: AtomicMediator, cb: Callback [Unit]): Unit =
      _abort (mdtr, tx, cb)

    def timeout(): Unit =
      throw new IllegalStateException

    def shutdown(): Unit =
      state = new Shutdown
  }

  object Prepared {

    def save (ops: Seq [WriteOp], tx: Transaction) = {
      openDb.put (xid.id, ())
      mainDb.put (xid.id, DeputyStatus.Prepared (tx.ft, ops))
      new Prepared (tx)
    }}

  class Committing (wt: TxClock) extends State {

    def prepare (mdtr: AtomicMediator, ct: TxClock, ops: Seq [WriteOp], cb: Callback [Unit]): Unit =
      _commit (mdtr, wt, ops, cb)

    def commit (mdtr: AtomicMediator, wt: TxClock, cb: Callback [Unit]) = ()

    def abort (mdtr: AtomicMediator, cb: Callback [Unit]): Unit =
      throw new IllegalStateException

    def timeout() = ()

    def shutdown(): Unit =
      state = new Shutdown

    override def toString = "Deputy.Committing"
  }

  class Aborting (ops: Seq [WriteOp], rsp: AtomicResponse) extends State {

    def prepare (mdtr: AtomicMediator, ct: TxClock, ops: Seq [WriteOp], cb: Callback [Unit]): Unit =
      _prepared (mdtr, rsp, cb)

    def commit (mdtr: AtomicMediator, wt: TxClock, cb: Callback [Unit]): Unit =
      _commit (mdtr, wt, ops, cb)

    def abort (mdtr: AtomicMediator, cb: Callback [Unit]): Unit =
      _abort (mdtr, cb)

    def timeout() = ()

    def shutdown(): Unit =
      state = new Shutdown

    override def toString = "Deputy.Committing"
  }

  class Committed private extends State {

    def prepare (mdtr: AtomicMediator, ct: TxClock, ops: Seq [WriteOp], cb: Callback [Unit]): Unit =
      _committed (mdtr, cb)

    def commit (mdtr: AtomicMediator, wt: TxClock, cb: Callback [Unit]): Unit =
      _committed (mdtr, cb)

    def abort (mdtr: AtomicMediator, cb: Callback [Unit]): Unit =
      throw new IllegalStateException

    def timeout() = ()

    def shutdown() =
      state = new Shutdown

    override def toString = "Deputy.Committed"
  }

  object Committed {

    def save() = {
      mainDb.put (xid.id, DeputyStatus.Committed)
      openDb.del (xid.id)
      new Committed
    }

    def forget() = new Committed
  }

  class Aborted private extends State {

    def prepare (mdtr: AtomicMediator, ct: TxClock, ops: Seq [WriteOp], cb: Callback [Unit]): Unit =
      _aborted (mdtr, cb)

    def commit (mdtr: AtomicMediator, wt: TxClock, cb: Callback [Unit]): Unit =
      throw new IllegalStateException


    def abort (mdtr: AtomicMediator, cb: Callback [Unit]): Unit =
      _aborted (mdtr, cb)

    def timeout() = ()

    def shutdown(): Unit =
      state = new Shutdown

    override def toString = "Deputy.Aborted"
  }

  object Aborted {

    def save() = {
      mainDb.put (xid.id, DeputyStatus.Aborted)
      openDb.del (xid.id)
      new Aborted
    }

    def forget() = new Aborted
  }

  class Shutdown extends State {

    def prepare (mdtr: AtomicMediator, ct: TxClock, ops: Seq [WriteOp], cb: Callback [Unit]) = cb()
    def commit (mdtr: AtomicMediator, wt: TxClock, cb: Callback [Unit]) = cb()
    def abort (mdtr: AtomicMediator, cb: Callback [Unit]) = cb()
    def timeout() = ()
    def shutdown() = ()

    override def toString = "Deputy.Shutdown"
  }


  def prepare (mdtr: AtomicMediator, ct: TxClock, ops: Seq [WriteOp]): Unit =
    fiber.begin (state.prepare (mdtr, ct, ops, _))

  def commit (mdtr: AtomicMediator, wt: TxClock): Unit =
    fiber.begin (state.commit (mdtr, wt, _))

  def abort (mdtr: AtomicMediator): Unit =
    fiber.begin (state.abort (mdtr, _))

  override def toString = state.toString
}

private object Deputy {

  val prepare = {
    import AtomicPicklers._
    new RequestDescriptor (
        0xFFDD52697F320AD1L,
        tuple (txId, txClock, seq (writeOp)),
        atomicResponse)
  }

  val commit = {
    import AtomicPicklers._
    new RequestDescriptor (0xFFF9E8BCFABDFFE6L, tuple (txId, txClock), atomicResponse)
  }

  val abort = {
    import AtomicPicklers._
    new RequestDescriptor (0xFF2D9D46D1F3A7F9L, txId, atomicResponse)
  }}
