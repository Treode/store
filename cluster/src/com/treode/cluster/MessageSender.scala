package com.treode.cluster

import com.treode.pickle.Pickler

trait MessageSender {
  def apply (h: Peer)
  def apply (hs: Iterable [Peer]) (implicit c: Cluster)
  def apply (acks: ReplyTracker) (implicit c: Cluster)
}

object MessageSender {

  def apply [M] (port: PortId, p: Pickler [M], msg: M): MessageSender =
    new MessageSender {

      def apply (h: Peer): Unit =
        h.send (p, port, msg)

      def apply (hs: Iterable [Peer]) (implicit c: Cluster): Unit =
        hs foreach (apply _)

      def apply (acks: ReplyTracker) (implicit c: Cluster): Unit =
        acks.awaiting foreach (h => apply (c.peer (h)))
    }}
