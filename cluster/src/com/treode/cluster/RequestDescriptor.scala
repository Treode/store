package com.treode.cluster

import com.treode.async.{Backoff, Fiber, Scheduler}
import com.treode.pickle.{Pickler, Picklers}

class RequestDescriptor [Q, A] private (id: PortId, preq: Pickler [Q], prsp: Pickler [A]) {

  type Port = EphemeralPort [A]
  type Mediator = RequestMediator [A]

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

  def open (f: (A, Peer) => Any) (implicit c: Cluster): Port =
    c.open (prsp) (f)
}

object RequestDescriptor {

  def apply [Q, A] (id: PortId, preq: Pickler [Q], pans: Pickler [A]): RequestDescriptor [Q, A] =
    new RequestDescriptor (id, preq, pans)
}
