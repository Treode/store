package com.treode.cluster

import scala.util.{Failure, Success, Try}

import com.treode.async.Async
import com.treode.pickle.{Pickler, Picklers}

import Async.guard

class RequestDescriptor [Q, A] private (id: PortId, preq: Pickler [Q], prsp: Pickler [A]) {

  type Port = EphemeralPort [Option [A]]

  private val _preq = {
    import Picklers._
    tuple (PortId.pickler, preq)
  }

  private val _prsp = {
    import Picklers._
    option (prsp)
  }

  def listen (f: (Q, Peer) => Async [A]) (implicit c: Cluster): Unit =
    c.listen (MessageDescriptor (id, _preq)) { case ((port, req), from) =>
      guard (f (req, from)) run {
        case Success (rsp) =>
          from.send (_prsp, port, Some (rsp))
        case Failure (_: IgnoreRequestException) =>
          ()
        case Failure (t) =>
          from.send (_prsp, port, None)
          throw t
      }}

  def apply (req: Q) = RequestSender [Q, A] (id, _preq, req)

  def open (f: (Try [A], Peer) => Any) (implicit c: Cluster): Port =
    c.open (_prsp) {
      case (Some (v), from) => f (Success (v), from)
      case (None, from) => f (Failure (new RemoteException), from)
    }

  override def toString = s"RequestDescriptor($id)"
}

object RequestDescriptor {

  def apply [Q, A] (id: PortId, preq: Pickler [Q], pans: Pickler [A]): RequestDescriptor [Q, A] =
    new RequestDescriptor (id, preq, pans)
}
