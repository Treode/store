package com.treode.cluster

import com.treode.pickle.Pickler

trait MessageSender {
  def apply (to: Peer)
  def apply (to: HostId) (implicit h: Host)
  def apply (to: Iterable [HostId]) (implicit h: Host)
}

object MessageSender {

  def apply [M] (mbx: MailboxId, p: Pickler [M], msg: M): MessageSender =
    new MessageSender {

      def apply (to: Peer): Unit =
        to.send (p, mbx, msg)

      def apply (to: HostId) (implicit h: Host): Unit =
        apply (h.peers.get (to))

      def apply (to: Iterable [HostId]) (implicit h: Host): Unit =
        to foreach (apply _)

      def apply (acks: Acknowledgements) (implicit h: Host): Unit =
        apply (acks.awaiting)
    }}
