package com.treode.cluster

import com.treode.pickle.Pickler

class RequestMediator [A] private [cluster] (prsp: Pickler [A], port: PortId, peer: Peer) {

  def respond (rsp: A): Unit =
    peer.send (prsp, port, rsp)

  override def toString = "RequestMediator" + (port, peer)
}
