package com.treode.cluster

import com.treode.pickle.Pickler

trait RequestSender [Rsp] {

  type Mailbox = EphemeralMailbox [Rsp]

  def apply (to: Peer, mbx: Mailbox)
  def apply (to: HostId, mbx: Mailbox) (implicit c: Cluster)
  def apply (to: Iterable [HostId], mbx: Mailbox) (implicit c: Cluster)
  def apply (acks: Acknowledgements, mbx: Mailbox) (implicit c: Cluster)
}

object RequestSender {

  def apply [Req, Rsp] (id: MailboxId, preq: Pickler [(MailboxId, Req)], req: Req): RequestSender [Rsp] =
    new RequestSender [Rsp] {

      private def sender (mbx: MailboxId) =
        MessageSender (id, preq, (mbx, req))

      def apply (to: Peer, mbx: Mailbox): Unit =
        sender (mbx.id) (to)

      def apply (to: HostId, mbx: Mailbox) (implicit c: Cluster): Unit =
        sender (mbx.id) (to)

      def apply (to: Iterable [HostId], mbx: Mailbox) (implicit c: Cluster): Unit =
        sender (mbx.id) (to)

      def apply (acks: Acknowledgements, mbx: Mailbox) (implicit c: Cluster): Unit =
        sender (mbx.id) (acks.awaiting)
    }}
