package com.treode.cluster

import com.treode.pickle.{Pickler, Picklers}

class RequestDescriptor [Req, Rsp] (id: MailboxId, req: Pickler [Req], rsp: Pickler [Rsp]) {

  private val _preq = {
    import Picklers._
    tuple (MailboxId.pickle, req)
  }

  type Mailbox = EphemeralMailbox [Rsp]
  type Mediator = RequestMediator [Rsp]

  def register (f: (Req, Mediator) => Any) (implicit h: Host): Unit =
    h.mailboxes.register (_preq, id) { case ((mbx, req), c) =>
      f (req, new RequestMediator (rsp, mbx, c))
    }

  def apply (req: Req) = RequestSender [Req, Rsp] (id, _preq, req)

  def open() (implicit h: Host): Mailbox =
    h.mailboxes.open (rsp)
}
