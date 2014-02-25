package com.treode.cluster

import com.treode.async.{Fiber, Scheduler}
import com.treode.pickle.{Pickler, Picklers}
import com.treode.cluster.misc.BackoffTimer

class RequestDescriptor [Q, A] private (id: MailboxId, preq: Pickler [Q], prsp: Pickler [A]) {

  type Mailbox = EphemeralMailbox [A]
  type Mediator = RequestMediator [A]

  abstract class QuorumCollector (req: Q) (acks: Acknowledgements, backoff: BackoffTimer) (
      implicit scheduler: Scheduler, cluster: Cluster) {

    private val fiber = new Fiber (scheduler)
    private val mbx = open (fiber)
    private val timer = backoff.iterator

    private def _send(): Unit = fiber.execute {
      if (!acks.quorum) {
        if (timer.hasNext) {
          apply (req) (acks, mbx)
          fiber.delay (timer.next) (_send())
        } else {
          mbx.close()
          timeout()
        }}}

    private def receive (f: A => Any) {
      mbx.receive { case (rsp, from) =>
        f (rsp)
        acks += from
        if (timer.hasNext) {
          if (acks.quorum) {
            mbx.close()
            quorum()
          } else {
            receive (f)
          }}}}

    def process (f: A => Any) {
      receive (f)
      _send()
    }

    def quorum()
    def timeout()
  }

  val void: Mediator =
    new Mediator (prsp, 0, Peer.void)

  private val _preq = {
    import Picklers._
    tuple (MailboxId.pickler, preq)
  }

  def listen (f: (Q, Mediator) => Any) (implicit c: Cluster): Unit =
    c.listen (MessageDescriptor (id, _preq)) { case ((mbx, req), c) =>
      f (req, new RequestMediator (prsp, mbx, c))
    }

  def apply (req: Q) = RequestSender [Q, A] (id, _preq, req)

  def open (s: Scheduler) (implicit c: Cluster): Mailbox =
    c.open (prsp, s)
}

object RequestDescriptor {

  def apply [Q, A] (id: MailboxId, preq: Pickler [Q], pans: Pickler [A]): RequestDescriptor [Q, A] =
    new RequestDescriptor (id, preq, pans)
}
