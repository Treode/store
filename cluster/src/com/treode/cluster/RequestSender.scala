package com.treode.cluster

import com.treode.pickle.Pickler

trait RequestSender [Rsp] {

  type Mailbox = EphemeralMailbox [Rsp]

  def apply (h: Peer, mbx: Mailbox)
  def apply (hs: Iterable [Peer], mbx: Mailbox) (implicit c: Cluster)
  def apply (acks: Acknowledgements, mbx: Mailbox) (implicit c: Cluster)
}

object RequestSender {

  def apply [Req, Rsp] (id: MailboxId, preq: Pickler [(MailboxId, Req)], req: Req): RequestSender [Rsp] =
    new RequestSender [Rsp] {

      private def send (mbx: MailboxId) =
        MessageSender (id, preq, (mbx, req))

      def apply (h: Peer, mbx: Mailbox): Unit =
        send (mbx.id) (h)

      def apply (hs: Iterable [Peer], mbx: Mailbox) (implicit c: Cluster): Unit =
        hs foreach (send (mbx.id) (_))

      def apply (acks: Acknowledgements, mbx: Mailbox) (implicit c: Cluster): Unit =
        acks.awaiting foreach (h => send (mbx.id) (c.peer (h)))
    }}
