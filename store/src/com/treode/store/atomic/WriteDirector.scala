package com.treode.store.atomic

import scala.collection.mutable
import scala.language.postfixOps
import scala.util.{Failure, Success}

import com.treode.async.{AsyncImplicits, Backoff, Callback, Fiber}
import com.treode.async.misc.RichInt
import com.treode.cluster.{Cluster, Peer}
import com.treode.store._

import AsyncImplicits._

private class WriteDirector (xid: TxId, ct: TxClock, ops: Seq [WriteOp], kit: AtomicKit) {
  import WriteDirector.deliberate
  import kit.{cluster, paxos, random, scheduler}
  import kit.config.prepareBackoff

  val fiber = new Fiber (scheduler)
  val port = cluster.open (WriteResponse.pickler) (receive _)
  val backoff = prepareBackoff.iterator
  var state: State = new Opening

  val cohorts = ops map (kit.locate (_))

  val prepares = TightTracker (ops, cohorts, kit) { (host, ops) =>
    WriteDeputy.prepare (xid, ct, ops) (host, port)
  }

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

    var failure = false
    var advance = false
    var ks = Set.empty [Int]
    var ft = TxClock.now

    prepares.rouse()
    fiber.delay (backoff.next) (state.timeout())

    private def maybeNextState() {
      if (prepares.quorum) {
        if (advance) {
          state = new Aborting (true)
          cb.pass (WriteResult.Stale)
        } else if (!ks.isEmpty) {
          state = new Aborting (true)
          cb.pass (WriteResult.Collided (ks.toSeq))
        } else if (failure) {
          state = new Aborting (true)
          cb.fail (new DeputyException)
        } else {
          state = new Deliberating (ft+1, cb)
        }}}

    override def prepared (ft: TxClock, from: Peer) {
      prepares += from
      if (this.ft < ft) this.ft = ft
      maybeNextState()
    }

    override def collisions (ks: Set [Int], from: Peer) {
      prepares += from
      this.ks ++= ks
      maybeNextState()
    }

    override def advance (from: Peer) {
      prepares += from
      advance = true
      maybeNextState()
    }

    override def failed (from: Peer) {
      prepares += from
      failure = true
      maybeNextState()
    }

    override def timeout() {
      if (backoff.hasNext) {
        prepares.rouse()
        fiber.delay (backoff.next) (state.timeout())
      } else {
        state = new Aborting (true)
        cb.pass (WriteResult.Timeout)
      }}

    override def toString = "Director.Preparing"
  }

  class Deliberating (wt: TxClock, cb: Callback [WriteResult]) extends State {
    import TxStatus._

    WriteDirector.deliberate.lead (xid.id, Committed (wt)) run {

      case Success (status) => fiber.execute {
        status match {
          case Committed (wt) =>
            state = new Committing (wt)
            cb.pass (WriteResult.Written (wt))
          case Aborted =>
            state = new Aborting (false)
            cb.pass (WriteResult.Timeout)
        }}

      case Failure (t) => fiber.execute {
        state = new Aborting (false)
        cb.fail (t)
      }}

    override def prepared (ft: TxClock, from: Peer): Unit =
      prepares += from

    override def timeout() {
      if (backoff.hasNext) {
        prepares.rouse()
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
        prepares.rouse()
        commits.rouse()
        fiber.delay (backoff.next) (state.timeout())
      } else {
        state = new Closed
      }}

    override def toString = "Director.Committing"
  }

  class Aborting (lead: Boolean) extends State {

    val aborts = BroadTracker (cohorts, kit) { hosts =>
      WriteDeputy.abort (xid) (hosts, port)
    }

    if (lead)
      WriteDirector.deliberate.lead (xid.id, TxStatus.Aborted) run (Callback.ignore)
    aborts.rouse()

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

  def receive (rsp: WriteResponse, from: Peer): Unit = fiber.execute {
    import WriteResponse._
    rsp match {
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
