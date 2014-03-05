package com.treode.cluster

import com.treode.pickle.Pickler

trait RequestSender [A] {

  type Port = EphemeralPort [A]

  def apply (h: Peer, port: Port)
  def apply (hs: Iterable [Peer], port: Port) (implicit c: Cluster)
  def apply (acks: ReplyTracker, port: Port) (implicit c: Cluster)
}

object RequestSender {

  def apply [Q, A] (id: PortId, preq: Pickler [(PortId, Q)], req: Q): RequestSender [A] =
    new RequestSender [A] {

      private def send (port: PortId) =
        MessageSender (id, preq, (port, req))

      def apply (h: Peer, port: Port): Unit =
        send (port.id) (h)

      def apply (hs: Iterable [Peer], port: Port) (implicit c: Cluster): Unit =
        hs foreach (send (port.id) (_))

      def apply (acks: ReplyTracker, port: Port) (implicit c: Cluster): Unit =
        acks.awaiting foreach (h => send (port.id) (c.peer (h)))
    }}
