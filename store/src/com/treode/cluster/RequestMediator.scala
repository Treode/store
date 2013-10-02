package com.treode.cluster

import com.treode.pickle.Pickler

class RequestMediator [Rsp] private [cluster] (prsp: Pickler [Rsp], mbx: MailboxId, peer: Peer) {

  def respond (rsp: Rsp): Unit =
    peer.send (prsp, mbx, rsp)

  override def toString = "RequestMediator" + (mbx, peer)
}
