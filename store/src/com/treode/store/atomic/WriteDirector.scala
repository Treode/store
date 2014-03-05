package com.treode.store.atomic

import java.util.concurrent.TimeoutException
import scala.collection.mutable
import scala.language.postfixOps

import com.treode.async.{Backoff, Callback, Fiber}
import com.treode.async.misc.RichInt
import com.treode.cluster.{Cluster, Peer}
import com.treode.store.{PaxosAccessor, TxClock, TxId, WriteOp, WriteResult}

private class WriteDirector (xid: TxId, ct: TxClock, ops: Seq [WriteOp], kit: AtomicKit) {
  import WriteDirector.deliberate
  import kit.{atlas, cluster, paxos, random, scheduler}

  val prepareBackoff = Backoff (100, 100, 1 seconds, 7) (random)
  val closedLifetime = 2 seconds

  val fiber = new Fiber (scheduler)
  val port = cluster.open (WriteResponse.pickler) (receive _)
  val backoff = prepareBackoff.iterator
  val prepares = atlas.locate (0)
  var state: State = new Opening

  trait State {

    def isOpen = true

    def open (cb: Callback [WriteResult]): Unit =
      throw new IllegalStateException

    def prepared (ft: TxClock, from: Peer) = ()

    def collisions (ks: Set [Int], from: Peer) = ()

    def advance (from: Peer) = ()

    def committed (from: Peer): Unit =
      throw new IllegalStateException

    def aborted (from: Peer): Unit =
      throw new IllegalStateException

    def failed (from: Peer): Unit =
      throw new IllegalStateException

    def timeout() = ()
  }

  class Opening extends State {

    override def open (cb: Callback [WriteResult]): Unit =
      state = new Preparing (cb)

    override def prepared (ft: TxClock, from: Peer): Unit =
      throw new IllegalStateException

    override def collisions (ks: Set [Int], from: Peer): Unit =
      throw new IllegalStateException

    override def advance (from: Peer): Unit =
      throw new IllegalStateException
  }

  class Preparing (cb: Callback [WriteResult]) extends State {

    val acks = atlas.locate (0)
    var advance = false
    var ks = Set.empty [Int]
    var ft = TxClock.now

    WriteDeputy.prepare (xid, ct, ops) (acks, port)
    fiber.delay (backoff.next) (state.timeout())

    private def maybeNextState() {
      if (acks.quorum) {
        if (prepares.quorum) {
          state = new Deliberating (ft+1, cb)
        } else if (advance) {
          state = new Aborting (true)
          cb.pass (WriteResult.Stale)
        } else if (!ks.isEmpty) {
          state = new Aborting (true)
          cb.pass (WriteResult.Collided (ks.toSeq))
        } else {
          state = new Aborting (true)
          cb.fail (new Exception)
        }}}

    override def prepared (ft: TxClock, from: Peer) {
      if (this.ft < ft) this.ft = ft
      prepares += from
      acks += from
      maybeNextState()
    }

    override def collisions (ks: Set [Int], from: Peer) {
      this.ks ++= ks
      acks += from
      maybeNextState()
    }

    override def advance (from: Peer) {
      advance = true
      acks += from
      maybeNextState()
    }

    override def failed (from: Peer) {
      acks += from
      maybeNextState()
    }

    override def timeout() {
      if (backoff.hasNext) {
        WriteDeputy.prepare (xid, ct, ops) (acks, port)
        fiber.delay (backoff.next) (state.timeout())
      } else {
        state = new Aborting (true)
        cb.fail (new TimeoutException)
      }}

    override def toString = "Director.Preparing"
  }

  class Deliberating (wt: TxClock, cb: Callback [WriteResult]) extends State {
    import TxStatus._

    WriteDirector.deliberate.lead (xid.id, Committed (wt)) run (new Callback [TxStatus] {

      def pass (status: TxStatus) = fiber.execute {
        status match {
          case Committed (wt) =>
            state = new Committing (wt)
            cb.pass (WriteResult.Written (wt))
          case Aborted =>
            state = new Aborting (false)
            cb.fail (new TimeoutException)
        }}

      def fail (t: Throwable) = fiber.execute {
        state = new Aborting (false)
        cb.fail (t)
      }})

    override def prepared (ft: TxClock, from: Peer): Unit =
      prepares += from

    override def timeout() {
      if (backoff.hasNext) {
        WriteDeputy.prepare (xid, ct, ops) (prepares, port)
        fiber.delay (backoff.next) (state.timeout())
      }}

    override def toString = "Director.Deliberating"
  }

  class Committing (wt: TxClock) extends State {

    val commits = atlas.locate (0)

    WriteDeputy.commit (xid, wt) (commits, port)
    fiber.delay (backoff.next) (state.timeout())

    override def prepared (ft: TxClock, from: Peer): Unit =
      prepares += from

    override def committed (from: Peer) {
      prepares += from
      commits += from
      if (commits.unity)
        state = new Closed
    }

    override def failed (from: Peer) = ()

    override def timeout() {
      if (backoff.hasNext) {
        WriteDeputy.prepare (xid, ct, ops) (prepares, port)
        WriteDeputy.commit (xid, wt) (commits, port)
        fiber.delay (backoff.next) (state.timeout())
      } else {
        state = new Closed
      }}

    override def toString = "Director.Committing"
  }

  class Aborting (lead: Boolean) extends State {

    val aborts = atlas.locate (0)

    if (lead)
      WriteDirector.deliberate.lead (xid.id, TxStatus.Aborted) run (Callback.ignore)
    WriteDeputy.abort (xid) (aborts, port)

    override def aborted (from: Peer) {
      aborts += from
      if (aborts.unity)
        state = new Closed
    }

    override def failed (from: Peer) = ()

    override def timeout() {
      if (backoff.hasNext) {
        WriteDeputy.abort (xid) (aborts, port)
        fiber.delay (backoff.next) (state.timeout())
      } else {
        state = new Closed
      }}

    override def toString = "Director.Aborting"
  }

  class Closed extends State {

    override def isOpen = false
    override def committed (from: Peer) = ()
    override def aborted (from: Peer) = ()
    override def failed (from: Peer) = ()

    override def toString = "Director.Closed"
  }

  def receive (msg: WriteResponse, from: Peer): Unit = fiber.execute {
    import WriteResponse._
    msg match {
      case Prepared (ft)   => state.prepared (ft, from)
      case Collisions (ks) => state.collisions (ks, from)
      case Advance         => state.advance (from)
      case Committed       => state.committed (from)
      case Aborted         => state.aborted (from)
      case Failed          => state.failed (from)
    }}

  def open (cb: Callback [WriteResult]): Unit =
    fiber.execute (state.open (cb))
}

private object WriteDirector {

  val deliberate = {
    import AtomicPicklers._
    PaxosAccessor.value (txStatus)
  }}
