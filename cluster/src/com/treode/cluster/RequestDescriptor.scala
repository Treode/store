package com.treode.cluster

import com.treode.async.{Backoff, Fiber, Scheduler}
import com.treode.pickle.{Pickler, Picklers}

class RequestDescriptor [Q, A] private (id: PortId, preq: Pickler [Q], prsp: Pickler [A]) {

  type Port = EphemeralPort [A]
  type Mediator = RequestMediator [A]

  abstract class QuorumCollector (req: Q) (acks: ReplyTracker, backoff: Backoff) (
      implicit scheduler: Scheduler, cluster: Cluster) {

    private val fiber = new Fiber (scheduler)
    private val port = open (fiber) (receive _)
    private val timer = backoff.iterator

    if (timer.hasNext)
      fiber.delay (timer.next) (_send())

    private def _send(): Unit = fiber.execute {
      if (!acks.quorum) {
        if (timer.hasNext) {
          apply (req) (acks, port)
          fiber.delay (timer.next) (_send())
        } else {
          port.close()
          timeout()
        }}}

    private def receive (rsp: A, from: Peer) {
      if (!acks.quorum) {
        process (rsp)
        acks += from
        if (timer.hasNext) {
          if (acks.quorum) {
            port.close()
            quorum()
          }}}}

    def process (rsp: A)
    def quorum()
    def timeout()
  }

  val void: Mediator =
    new Mediator (prsp, 0, Peer.void)

  private val _preq = {
    import Picklers._
    tuple (PortId.pickler, preq)
  }

  def listen (f: (Q, Mediator) => Any) (implicit c: Cluster): Unit =
    c.listen (MessageDescriptor (id, _preq)) { case ((port, req), c) =>
      f (req, new RequestMediator (prsp, port, c))
    }

  def apply (req: Q) = RequestSender [Q, A] (id, _preq, req)

  def open (s: Scheduler) (f: (A, Peer) => Any) (implicit c: Cluster): Port =
    c.open (prsp) (f)
}

object RequestDescriptor {

  def apply [Q, A] (id: PortId, preq: Pickler [Q], pans: Pickler [A]): RequestDescriptor [Q, A] =
    new RequestDescriptor (id, preq, pans)
}
