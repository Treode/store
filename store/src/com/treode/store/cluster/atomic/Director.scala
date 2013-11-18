package com.treode.store.cluster.atomic

import java.util.concurrent.TimeoutException
import scala.collection.mutable
import scala.language.postfixOps

import com.treode.cluster.{Acknowledgements, Host, Peer}
import com.treode.cluster.misc.{BackoffTimer, RichInt}
import com.treode.concurrent.{Callback, Fiber}
import com.treode.store._

private class Director (batch: WriteBatch, kit: AtomicKit) {
  import Director.deliberate
  import kit.{host, paxos}
  import kit.host.{mailboxes, random, scheduler}

  val prepareBackoff = BackoffTimer (100, 100, 1 seconds, 7) (random)
  val closedLifetime = 2 seconds

  val fiber = new Fiber (scheduler)
  val mbx = mailboxes.open (AtomicResponse.pickle, fiber)
  var state: State = new Opening

  val backoff = prepareBackoff.iterator
  val prepares = host.locate (0)

  trait State {

    def isOpen = true

    def open (cb: WriteCallback): Unit =
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

    override def open (cb: WriteCallback): Unit =
      state = new Preparing (cb)

    override def prepared (ft: TxClock, from: Peer): Unit =
      throw new IllegalStateException

    override def collisions (ks: Set [Int], from: Peer): Unit =
      throw new IllegalStateException

    override def advance (from: Peer): Unit =
      throw new IllegalStateException
  }

  class Preparing (cb: WriteCallback) extends State {

    val acks = host.locate (0)
    var advance = false
    var ks = Set.empty [Int]
    var ft = batch.ft

    Deputy.prepare (batch) (acks, mbx)
    fiber.delay (backoff.next) (state.timeout())

    private def maybeNextState() {
      if (acks.quorum) {
        if (prepares.quorum) {
          state = new Deliberating (ft+1, cb)
        } else if (advance) {
          cb.advance()
          state = new Aborting (true)
        } else if (!ks.isEmpty) {
          cb.collisions (ks)
          state = new Aborting (true)
        } else {
          cb.fail (new Exception)
          state = new Aborting (true)
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
        Deputy.prepare (batch) (acks, mbx)
        fiber.delay (backoff.next) (state.timeout())
      } else {
        cb.fail (new TimeoutException)
        state = new Aborting (true)
      }}

    override def toString = "Director.Preparing"
  }

  class Deliberating (wt: TxClock, cb: WriteCallback) extends State {
    import AtomicStatus._

    Director.deliberate.lead (batch.xid.id, Committed (wt), new Callback [AtomicStatus] {

      def pass (status: AtomicStatus) = fiber.execute {
        status match {
          case Committed (wt) =>
            cb (wt)
            state = new Committing (wt)
          case Aborted =>
            cb.fail (new TimeoutException)
            state = new Aborting (false)
        }}

      def fail (t: Throwable) = fiber.execute {
        cb.fail (t)
        state = new Aborting (false)
      }})

    override def prepared (ft: TxClock, from: Peer): Unit =
      prepares += from

    override def timeout() {
      if (backoff.hasNext) {
        Deputy.prepare (batch) (prepares, mbx)
        fiber.delay (backoff.next) (state.timeout())
      }}

    override def toString = "Director.Deliberating"
  }

  class Committing (wt: TxClock) extends State {

    val commits = host.locate (0)

    Deputy.commit (batch.xid, wt) (commits, mbx)
    fiber.delay (backoff.next) (state.timeout())

    override def prepared (ft: TxClock, from: Peer): Unit =
      prepares += from

    override def committed (from: Peer) {
      prepares += from
      commits += from
      if (commits.unity)
        state = new Closed
    }

    override def timeout() {
      if (backoff.hasNext) {
        Deputy.prepare (batch) (prepares, mbx)
        Deputy.commit (batch.xid, wt) (commits, mbx)
        fiber.delay (backoff.next) (state.timeout())
      } else {
        state = new Closed
      }}

    override def toString = "Director.Committing"
  }

  class Aborting (lead: Boolean) extends State {

    val aborts = host.locate (0)

    if (lead)
      Director.deliberate.lead (batch.xid.id, AtomicStatus.Aborted, Callback.ignore)
    Deputy.abort (batch.xid) (aborts, mbx)

    override def aborted (from: Peer) {
      aborts += from
      if (aborts.unity)
        state = new Closed
    }

    override def timeout() {
      if (backoff.hasNext) {
        Deputy.abort (batch.xid) (aborts, mbx)
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

    override def toString = "Director.Closed"
  }

  mbx.whilst (state.isOpen) { (msg, from) =>
    import AtomicResponse._
    msg match {
      case Prepared (ft)   => state.prepared (ft, from)
      case Collisions (ks) => state.collisions (ks, from)
      case Advance         => state.advance (from)
      case Committed       => state.committed (from)
      case Aborted         => state.aborted (from)
      case Failed          => state.failed (from)
    }}

  def open (cb: WriteCallback): Unit =
    fiber.execute (state.open (cb))
}

private object Director {

  val deliberate = {
    import AtomicPicklers._
    PaxosAccessor.value (atomicStatus)
  }}
