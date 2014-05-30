package com.treode.store.atomic

import scala.collection.mutable
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

import com.treode.async.{Async, Backoff, Callback, Fiber}
import com.treode.async.implicits._
import com.treode.async.misc.RichInt
import com.treode.cluster.{Cluster, Peer}
import com.treode.store._
import com.treode.store.paxos.PaxosAccessor

import Async.async
import WriteDirector.deliberate

private class WriteDirector (xid: TxId, ct: TxClock, ops: Seq [WriteOp], kit: AtomicKit) {
  import kit.{cluster, library, paxos, random, scheduler}
  import kit.config.prepareBackoff

  val fiber = new Fiber
  val port = WriteDeputy.prepare.open (receive _)
  val backoff = prepareBackoff.iterator
  var state: State = new Opening

  val atlas = library.atlas
  val cohorts = ops map (op => locate (atlas, op.table, op.key))

  val responses = TightTracker (ops, cohorts, kit) { (host, ops) =>
    WriteDeputy.prepare (atlas.version, xid, ct, ops) (host, port)
  }

  trait State {

    def isOpen = true

    def open (cb: Callback [TxClock]): Unit =
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

    override def open (cb: Callback [TxClock]): Unit =
      state = new Preparing (cb)

    override def prepared (ft: TxClock, from: Peer): Unit =
      throw new IllegalStateException

    override def collisions (ks: Set [Int], from: Peer): Unit =
      throw new IllegalStateException

    override def advance (from: Peer): Unit =
      throw new IllegalStateException
  }

  class Preparing (cb: Callback [TxClock]) extends State {

    var failure = false
    var advance = false
    var ks = Set.empty [Int]
    var ft = TxClock.now

    responses.rouse()
    fiber.delay (backoff.next) (state.timeout())

    private def maybeNextState() {
      if (responses.quorum) {
        if (advance) {
          state = new Aborting
          cb.fail (new StaleException)
        } else if (!ks.isEmpty) {
          state = new Aborting
          cb.fail (new CollisionException (ks.toSeq))
        } else if (failure) {
          state = new Aborting
          cb.fail (new RemoteException)
        } else {
          state = new Deliberating (ft+1, cb)
        }}}

    override def prepared (ft: TxClock, from: Peer) {
      responses += from
      if (this.ft < ft) this.ft = ft
      maybeNextState()
    }

    override def collisions (ks: Set [Int], from: Peer) {
      responses += from
      this.ks ++= ks
      maybeNextState()
    }

    override def advance (from: Peer) {
      responses += from
      advance = true
      maybeNextState()
    }

    override def failed (from: Peer) {
      responses += from
      failure = true
      maybeNextState()
    }

    override def timeout() {
      if (backoff.hasNext) {
        responses.rouse()
        fiber.delay (backoff.next) (state.timeout())
      } else {
        state = new Aborting
        cb.fail (new TimeoutException)
      }}

    override def toString = "Director.Preparing"
  }

  class Deliberating (wt: TxClock, cb: Callback [TxClock]) extends State {
    import TxStatus._

    _commit()

    def _commit() {
      deliberate.lead (xid.id, xid.time, Committed (wt)) run {

        case Success (status) => fiber.execute {
          status match {
            case Committed (wt) =>
              state = new Committing (wt)
              cb.pass (wt)
            case Aborted =>
              state = new Aborting
              cb.fail (new TimeoutException)
          }}

        case Failure (t) =>
          _commit()
      }}

    override def prepared (ft: TxClock, from: Peer): Unit =
      responses += from

    override def timeout() {
      if (backoff.hasNext) {
        responses.rouse()
        fiber.delay (backoff.next) (state.timeout())
      }}

    override def toString = "Director.Deliberating"
  }

  class Committing (wt: TxClock) extends State {

    val commits = BroadTracker (cohorts, kit) { hosts =>
      WriteDeputy.commit (xid, wt) (hosts, port)
    }

    commits.rouse()
    fiber.delay (backoff.next) (state.timeout())

    override def prepared (ft: TxClock, from: Peer): Unit =
      responses += from

    override def committed (from: Peer) {
      responses += from
      commits += from
      if (commits.unity)
        state = new Closed
    }

    override def failed (from: Peer) = ()

    override def timeout() {
      if (backoff.hasNext) {
        responses.rouse()
        commits.rouse()
        fiber.delay (backoff.next) (state.timeout())
      } else {
        state = new Closed
      }}

    override def toString = "Director.Committing"
  }

  class Aborting extends State {

    val aborts = BroadTracker (cohorts, kit) { hosts =>
      WriteDeputy.abort (xid) (hosts, port)
    }

    _abort()
    aborts.rouse()
    fiber.delay (backoff.next) (state.timeout())

    def _abort() {
      deliberate.lead (xid.id, xid.time, TxStatus.Aborted) run {
        case Success (_) => ()
        case Failure (_) => _abort()
      }}

    override def aborted (from: Peer) {
      aborts += from
      if (aborts.unity)
        state = new Closed
    }

    override def failed (from: Peer) = ()

    override def timeout() {
      if (backoff.hasNext) {
        aborts.rouse()
        fiber.delay (backoff.next) (state.timeout())
      } else {
        state = new Closed
      }}

    override def toString = "Director.Aborting"
  }

  class Closed extends State {

    port.close()

    override def isOpen = false
    override def committed (from: Peer) = ()
    override def aborted (from: Peer) = ()
    override def failed (from: Peer) = ()

    override def toString = "Director.Closed"
  }

  def receive (rsp: Try [WriteResponse], from: Peer): Unit = fiber.execute {
    import WriteResponse._
    rsp match {
      case Success (Prepared (ft))   => state.prepared (ft, from)
      case Success (Collisions (ks)) => state.collisions (ks, from)
      case Success (Advance)         => state.advance (from)
      case Success (Committed)       => state.committed (from)
      case Success (Aborted)         => state.aborted (from)
      case Success (Failed)          => state.failed (from)
      case Failure (_)               => state.failed (from)
    }}

  def open (cb: Callback [TxClock]): Unit =
    fiber.execute (state.open (cb))
}

private object WriteDirector {

  val deliberate = {
    import AtomicPicklers._
    PaxosAccessor.value (txStatus)
  }

  def write (xid: TxId, ct: TxClock, ops: Seq [WriteOp], kit: AtomicKit): Async [TxClock] =
    async (cb => new WriteDirector (xid, ct, ops, kit) .open (cb))
}
