package com.treode.store.atomic

import com.treode.async.{Callback, Fiber}
import com.treode.cluster.{RequestDescriptor, RequestMediator}
import com.treode.store.{PrepareCallback, Transaction, TxClock, TxId, WriteOp}

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
      state = new Committing (mdtr, wt)

    def timeout(): Unit =
      throw new IllegalStateException

    def shutdown(): Unit =
      state = new Shutdown

    override def toString = "Deputy.Open"
  }

  class Preparing (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp]) extends State {

    private var committed = Option.empty [(WriteMediator, TxClock)]
    private var aborted = Option.empty [WriteMediator]

    store.prepare (ct, ops, new PrepareCallback {

      def pass (tx: Transaction): Unit = fiber.execute {
        if (committed.isDefined) {
          val Some ((mdtr2, wt)) = committed
          state = new Committing2 (mdtr2, wt, ops)
          mdtr.respond (WriteResponse.Prepared (tx.ft))
        } else if (aborted.isDefined) {
          val Some (mdtr2) = aborted
          state = new Aborted
          tx.abort()
          mdtr2.respond (WriteResponse.Aborted)
        } else {
          state = new Prepared (tx)
          mdtr.respond (WriteResponse.Prepared (tx.ft))
        }}

      def fail (t: Throwable): Unit = fiber.execute {
        if (committed.isDefined) {
          val Some ((mdtr2, wt)) = committed
          state = new Committing2 (mdtr2, wt, ops)
        } else if (aborted.isDefined) {
          val Some (mdtr2) = aborted
          state = new Aborted
          mdtr2.respond (WriteResponse.Aborted)
        } else {
          state = new Deliberating (ops, WriteResponse.Failed)
          mdtr.respond (WriteResponse.Failed)
        }}

      def collisions (ks: Set [Int]): Unit = fiber.execute {
        if (committed.isDefined) {
          val Some ((mdtr2, wt)) = committed
          state = new Committing2 (mdtr2, wt, ops)
        } else if (aborted.isDefined) {
          val Some (mdtr2) = aborted
          state = new Aborted
          mdtr2.respond (WriteResponse.Aborted)
        } else {
          val rsp = WriteResponse.Collisions (ks)
          state = new Deliberating (ops, rsp)
          mdtr.respond (rsp)
        }}

      def advance(): Unit = fiber.execute {
        if (committed.isDefined) {
          val Some ((mdtr2, wt)) = committed
          state = new Committing2 (mdtr2, wt, ops)
        } else if (aborted.isDefined) {
          val Some (mdtr2) = aborted
          state = new Aborted
          mdtr2.respond (WriteResponse.Aborted)
        } else {
          state = new Deliberating (ops, WriteResponse.Advance)
          mdtr.respond (WriteResponse.Advance)
        }}})

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp]): Unit = ()

    def commit (mdtr: WriteMediator, wt: TxClock): Unit =
      committed = Some ((mdtr, wt))

    def abort (mdtr: WriteMediator): Unit =
      aborted = Some (mdtr)

    def timeout(): Unit =
      throw new IllegalStateException

    def shutdown(): Unit =
      state = new Shutdown
  }

  class Prepared (tx: Transaction) extends State {

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp]): Unit =
      mdtr.respond (WriteResponse.Prepared (tx.ft))

    def commit (mdtr: WriteMediator, wt: TxClock): Unit =
      new Committing3 (mdtr, wt, tx)

    def abort (mdtr: WriteMediator) {
      state = new Aborted
      tx.abort()
      mdtr.respond (WriteResponse.Aborted)
    }

    def timeout(): Unit =
      throw new IllegalStateException

    def shutdown(): Unit =
      state = new Shutdown
  }

  class Deliberating (ops: Seq [WriteOp], rsp: WriteResponse) extends State {

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp]): Unit =
      mdtr.respond (rsp)

    def commit (mdtr: WriteMediator, wt: TxClock): Unit =
      new Committing2 (mdtr, wt, ops)

    def abort (mdtr: WriteMediator): Unit =
      state = new Aborted

    def timeout() = ()

    def shutdown(): Unit =
      state = new Shutdown

    override def toString = "Deputy.Deliberating"
  }

  class Committing (mdtr: WriteMediator, wt: TxClock) extends State {

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp]): Unit =
      new Committing2 (mdtr, wt, ops)

    def commit (mdtr: WriteMediator, wt: TxClock) = ()

    def abort (mdtr: WriteMediator): Unit =
      throw new IllegalStateException

    def timeout() = ()

    def shutdown(): Unit =
      state = new Shutdown

    override def toString = "Deputy.Committing"
  }

  class Committing2 (mdtr: WriteMediator, wt: TxClock, ops: Seq [WriteOp]) extends State {

    store.commit (wt, ops, new Callback [Unit] {
      def pass (v: Unit) = fiber.execute {
        state = new Committed
        mdtr.respond (WriteResponse.Committed)
      }
      def fail (t: Throwable) = fiber.execute {
        state = new Panicked
        throw t
      }})

    def prepare (mdtr: WriteMediator, ct: TxClock, ops: Seq [WriteOp]): Unit = ()

    def commit (mdtr: WriteMediator, wt: TxClock): Unit = ()

    def abort (mdtr: WriteMediator): Unit =
      throw new IllegalStateException

    def timeout() = ()

    def shutdown(): Unit =
      state = new Shutdown

    override def toString = "Deputy.Committing2"
  }

  class Committing3 (mdtr: WriteMediator, wt: TxClock, tx: Transaction) extends State {

    tx.commit (wt, new Callback [Unit] {
      def pass (v: Unit) = fiber.execute {
        state = new Committed
        mdtr.respond (WriteResponse.Committed)
      }
      def fail (t: Throwable) = fiber.execute {
        state = new Panicked
        throw t
      }})

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
