package com.treode.store.atomic

import com.treode.async.{Callback, Fiber}
import com.treode.cluster.{RequestDescriptor, RequestMediator}
import com.treode.store.{Preparation, PrepareCallback, TxClock, TxId, WriteOp}

private class WriteDeputy (xid: TxId, kit: AtomicKit) {
  import kit.{scheduler, store}

  type WriteMediator = RequestMediator [WriteResponse]

  val fiber = new Fiber (scheduler)
  var state: State = new Restoring

  trait State {
    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp])
    def commit (mdtr: WriteMediator, wt: TxClock)
    def abort (mdtr: WriteMediator)
    def timeout()
    def shutdown()
  }

  class Restoring extends State {

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp]) {
      state = new Open
      state.prepare (mdtr, ct, ops)
    }

    def abort (mdtr: WriteMediator) {
      state = new Open
      state.abort (mdtr)
    }

    def commit (mdtr: WriteMediator, wxt: TxClock) {
      state = new Open
      state.commit (mdtr, wxt)
    }

    def timeout(): Unit =
      throw new IllegalStateException

    def shutdown(): Unit =
      throw new IllegalStateException

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

    store.prepare (ct, ops, new PrepareCallback {

      def pass (prep: Preparation): Unit = fiber.execute {
        if (state == Preparing.this) {
          state = new Prepared (ops, prep)
          mdtr.respond (WriteResponse.Prepared (prep.ft))
        } else {
          prep.release()
        }}

      def fail (t: Throwable): Unit = fiber.execute {
        if (state == Preparing.this) {
          state = new Deliberating (ops, WriteResponse.Failed)
          mdtr.respond (WriteResponse.Failed)
        }}

      def collisions (ks: Set [Int]): Unit = fiber.execute {
        if (state == Preparing.this) {
          val rsp = WriteResponse.Collisions (ks)
          state = new Deliberating (ops, rsp)
          mdtr.respond (rsp)
        }}

      def advance(): Unit = fiber.execute {
        if (state == Preparing.this) {
          state = new Deliberating (ops, WriteResponse.Advance)
          mdtr.respond (WriteResponse.Advance)
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

  class Prepared (ops: Seq [WriteOp], prep: Preparation) extends State {

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp]): Unit =
      mdtr.respond (WriteResponse.Prepared (prep.ft))

    def commit (mdtr: WriteMediator, wt: TxClock): Unit =
      state = new Committing (mdtr, wt, ops, Some (prep))

    def abort (mdtr: WriteMediator) {
      state = new Aborted
      prep.release()
      mdtr.respond (WriteResponse.Aborted)
    }

    def timeout(): Unit =
      throw new IllegalStateException

    def shutdown() {
      state = new Shutdown
      prep.release()
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
      prep: Option [Preparation]) extends State {

    store.commit (wt, ops, new Callback [Unit] {

      def pass (v: Unit) = fiber.execute {
        prep foreach (_.release())
        if (state == Committing.this) {
          state = new Committed
          mdtr.respond (WriteResponse.Committed)
        }}

      def fail (t: Throwable) = fiber.execute {
        prep foreach (_.release())
        if (state == Committing.this) {
          state = new Panicked
          throw t
        }}})

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp]): Unit = ()

    def commit (mdtr: WriteMediator, wt: TxClock): Unit = ()

    def abort (mdtr: WriteMediator): Unit =
      throw new IllegalStateException

    def timeout() = ()

    def shutdown(): Unit =
      state = new Shutdown

    override def toString = "Deputy.Committing2"
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
  }}
