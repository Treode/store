package com.treode.cluster

import com.treode.pickle.Pickler

trait MessageSender {
  def apply (to: Peer)
  def apply (to: HostId) (implicit c: Cluster)
  def apply (to: Iterable [HostId]) (implicit c: Cluster)
  def apply (acks: Acknowledgements) (implicit c: Cluster)
}

object MessageSender {

  def apply [M] (mbx: MailboxId, p: Pickler [M], msg: M): MessageSender =
    new MessageSender {

      def apply (to: Peer): Unit =
        to.send (p, mbx, msg)

      def apply (to: HostId) (implicit c: Cluster): Unit =
        apply (c.peer (to))

      def apply (to: Iterable [HostId]) (implicit c: Cluster): Unit =
        to foreach (apply _)

      def apply (acks: Acknowledgements) (implicit c: Cluster): Unit =
        apply (acks.awaiting)
    }}
