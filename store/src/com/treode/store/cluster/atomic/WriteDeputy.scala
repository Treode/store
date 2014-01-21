package com.treode.store.cluster.atomic

import com.treode.async.{Callback, Fiber, callback, guard}
import com.treode.cluster.{RequestDescriptor, RequestMediator}
import com.treode.store.{PrepareCallback, Transaction, TxClock, TxId, WriteOp}

private class WriteDeputy (xid: TxId, kit: AtomicKit) {
  import kit.WriteDeputies.{mainDb, openDb}
  import kit.{scheduler, store}

  type WriteMediator = RequestMediator [WriteResponse]

  val fiber = new Fiber (scheduler)
  var state: State = new Restoring

  trait State {
    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp], cb: Callback [Unit])
    def commit (mdtr: WriteMediator, wt: TxClock, cb: Callback [Unit])
    def abort (mdtr: WriteMediator, cb: Callback [Unit])
    def timeout()
    def shutdown()
  }

  def _prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp], cb: Callback [Unit]): Unit =
    guard (cb) {
      store.prepare (ct, ops, new PrepareCallback {

        def pass (tx: Transaction) {
          state = Prepared.save (ops, tx)
          mdtr.respond (WriteResponse.Prepared (tx.ft))
          cb()
        }

        def fail (t: Throwable) {
          state = new Deliberating (ops, WriteResponse.Failed)
          mdtr.respond (WriteResponse.Failed)
          cb()
        }

        def collisions (ks: Set [Int]) {
          val rsp = WriteResponse.Collisions (ks)
          state = new Deliberating (ops, rsp)
          mdtr.respond (rsp)
          cb()
        }

        def advance() {
          state = new Deliberating (ops, WriteResponse.Advance)
          mdtr.respond (WriteResponse.Advance)
          cb()
        }})
    }

  def _prepared (mdtr: WriteMediator, rsp: WriteResponse, cb: Callback [Unit]): Unit =
    guard (cb) {
      mdtr.respond (rsp)
      cb()
    }

  def _commit (mdtr: WriteMediator, wt: TxClock, ops: Seq [WriteOp], cb: Callback [Unit]): Unit =
    store.commit (wt, ops, callback (cb) { _ =>
      state = Committed.save()
      mdtr.respond (WriteResponse.Committed)
    })

  def _commit (mdtr: WriteMediator, tx: Transaction, wt: TxClock, cb: Callback [Unit]): Unit =
    tx.commit (wt, callback (cb) { _ =>
      state = Committed.save()
      mdtr.respond (WriteResponse.Committed)
    })

  def _committed (mdtr: WriteMediator, cb: Callback [Unit]): Unit =
    guard (cb) {
      mdtr.respond (WriteResponse.Committed)
      cb()
    }

  def _abort (mdtr: WriteMediator, cb: Callback [Unit]): Unit =
    guard (cb) {
      state = Aborted.save()
      mdtr.respond (WriteResponse.Aborted)
      cb()
    }

  def _abort (mdtr: WriteMediator, tx: Transaction, cb: Callback [Unit]): Unit =
    guard (cb) {
      tx.abort()
      state = Aborted.save()
      mdtr.respond (WriteResponse.Aborted)
      cb()
    }

  def _aborted (mdtr: WriteMediator, cb: Callback [Unit]): Unit =
    guard (cb) {
      mdtr.respond (WriteResponse.Aborted)
      cb()
    }

  class Restoring extends State {

    def restore (cb: Callback [Unit]) (f: State => Unit) {
      guard (cb) {
        mainDb.get (xid.id, callback {
          case Some (WriteStatus.Prepared (ft, ops)) =>
            state = new Deliberating (ops, WriteResponse.Prepared (ft))
            f (state)
          case Some (WriteStatus.Committed) =>
            state = Committed.forget()
            f (state)
          case Some (WriteStatus.Aborted) =>
            state = Aborted.forget()
            f (state)
          case None =>
            state = new Open
            f (state)
        })
      }}

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp], cb: Callback [Unit]): Unit =
      restore (cb) (_.prepare (mdtr, ct, ops, cb))

    def abort (mdtr: WriteMediator, cb: Callback [Unit]): Unit =
      restore (cb) (_.abort (mdtr, cb))

    def commit (mdtr: WriteMediator, wxt: TxClock, cb: Callback [Unit]): Unit =
      restore (cb) (_.commit (mdtr, wxt, cb))

    def timeout(): Unit =
      throw new IllegalStateException

    def shutdown(): Unit =
      throw new IllegalStateException

    override def toString = "Deputy.Restoring"
  }

  class Open extends State {

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp], cb: Callback [Unit]): Unit =
      _prepare (mdtr, ct, ops, cb)

    def abort (mdtr: WriteMediator, cb: Callback [Unit]): Unit =
      _abort (mdtr, cb)

    def commit (mdtr: WriteMediator, wt: TxClock, cb: Callback [Unit]): Unit =
      guard (cb) {
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

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp], cb: Callback [Unit]): Unit =
      _prepared (mdtr, WriteResponse.Prepared (tx.ft), cb)

    def commit (mdtr: WriteMediator, wt: TxClock, cb: Callback [Unit]): Unit =
      _commit (mdtr, tx, wt, cb)

    def abort (mdtr: WriteMediator, cb: Callback [Unit]): Unit =
      _abort (mdtr, tx, cb)

    def timeout(): Unit =
      throw new IllegalStateException

    def shutdown(): Unit =
      state = new Shutdown
  }

  object Prepared {

    def save (ops: Seq [WriteOp], tx: Transaction) = {
      openDb.put (xid.id, ())
      mainDb.put (xid.id, WriteStatus.Prepared (tx.ft, ops))
      new Prepared (tx)
    }}

  class Committing (wt: TxClock) extends State {

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp], cb: Callback [Unit]): Unit =
      _commit (mdtr, wt, ops, cb)

    def commit (mdtr: WriteMediator, wt: TxClock, cb: Callback [Unit]) = ()

    def abort (mdtr: WriteMediator, cb: Callback [Unit]): Unit =
      throw new IllegalStateException

    def timeout() = ()

    def shutdown(): Unit =
      state = new Shutdown

    override def toString = "Deputy.Committing"
  }

  class Deliberating (ops: Seq [WriteOp], rsp: WriteResponse) extends State {

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp], cb: Callback [Unit]): Unit =
      _prepared (mdtr, rsp, cb)

    def commit (mdtr: WriteMediator, wt: TxClock, cb: Callback [Unit]): Unit =
      _commit (mdtr, wt, ops, cb)

    def abort (mdtr: WriteMediator, cb: Callback [Unit]): Unit =
      _abort (mdtr, cb)

    def timeout() = ()

    def shutdown(): Unit =
      state = new Shutdown

    override def toString = "Deputy.Committing"
  }

  class Committed private extends State {

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp], cb: Callback [Unit]): Unit =
      _committed (mdtr, cb)

    def commit (mdtr: WriteMediator, wt: TxClock, cb: Callback [Unit]): Unit =
      _committed (mdtr, cb)

    def abort (mdtr: WriteMediator, cb: Callback [Unit]): Unit =
      throw new IllegalStateException

    def timeout() = ()

    def shutdown() =
      state = new Shutdown

    override def toString = "Deputy.Committed"
  }

  object Committed {

    def save() = {
      mainDb.put (xid.id, WriteStatus.Committed)
      openDb.del (xid.id)
      new Committed
    }

    def forget() = new Committed
  }

  class Aborted private extends State {

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp], cb: Callback [Unit]): Unit =
      _aborted (mdtr, cb)

    def commit (mdtr: WriteMediator, wt: TxClock, cb: Callback [Unit]): Unit =
      throw new IllegalStateException


    def abort (mdtr: WriteMediator, cb: Callback [Unit]): Unit =
      _aborted (mdtr, cb)

    def timeout() = ()

    def shutdown(): Unit =
      state = new Shutdown

    override def toString = "Deputy.Aborted"
  }

  object Aborted {

    def save() = {
      mainDb.put (xid.id, WriteStatus.Aborted)
      openDb.del (xid.id)
      new Aborted
    }

    def forget() = new Aborted
  }

  class Shutdown extends State {

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp], cb: Callback [Unit]) = cb()
    def commit (mdtr: WriteMediator, wt: TxClock, cb: Callback [Unit]) = cb()
    def abort (mdtr: WriteMediator, cb: Callback [Unit]) = cb()
    def timeout() = ()
    def shutdown() = ()

    override def toString = "Deputy.Shutdown"
  }

  def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp]): Unit =
    fiber.begin (state.prepare (mdtr, ct, ops, _))

  def commit (mdtr: WriteMediator, wt: TxClock): Unit =
    fiber.begin (state.commit (mdtr, wt, _))

  def abort (mdtr: WriteMediator): Unit =
    fiber.begin (state.abort (mdtr, _))

  override def toString = state.toString
}

private object WriteDeputy {

  val prepare = {
    import AtomicPicklers._
    new RequestDescriptor (
        0xFFDD52697F320AD1L,
        tuple (txId, txClock, seq (writeOp)),
        writeResponse)
  }

  val commit = {
    import AtomicPicklers._
    new RequestDescriptor (0xFFF9E8BCFABDFFE6L, tuple (txId, txClock), writeResponse)
  }

  val abort = {
    import AtomicPicklers._
    new RequestDescriptor (0xFF2D9D46D1F3A7F9L, txId, writeResponse)
  }}
