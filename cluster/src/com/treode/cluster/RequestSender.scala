package com.treode.cluster

import com.treode.pickle.Pickler

trait RequestSender [A] {

  type Mailbox = EphemeralMailbox [A]

  def apply (h: Peer, mbx: Mailbox)
  def apply (hs: Iterable [Peer], mbx: Mailbox) (implicit c: Cluster)
  def apply (acks: ReplyTracker, mbx: Mailbox) (implicit c: Cluster)
}

object RequestSender {

  def apply [Q, A] (id: MailboxId, preq: Pickler [(MailboxId, Q)], req: Q): RequestSender [A] =
    new RequestSender [A] {

      private def send (mbx: MailboxId) =
        MessageSender (id, preq, (mbx, req))

      def apply (h: Peer, mbx: Mailbox): Unit =
        send (mbx.id) (h)

      def apply (hs: Iterable [Peer], mbx: Mailbox) (implicit c: Cluster): Unit =
        hs foreach (send (mbx.id) (_))

      def apply (acks: ReplyTracker, mbx: Mailbox) (implicit c: Cluster): Unit =
        acks.awaiting foreach (h => send (mbx.id) (c.peer (h)))
    }}
