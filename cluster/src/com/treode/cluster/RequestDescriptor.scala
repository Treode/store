package com.treode.cluster

import com.treode.async.{Backoff, Fiber, Scheduler}
import com.treode.pickle.{Pickler, Picklers}

class RequestDescriptor [Q, A] private (id: MailboxId, preq: Pickler [Q], prsp: Pickler [A]) {

  type Mailbox = EphemeralMailbox [A]
  type Mediator = RequestMediator [A]

  abstract class QuorumCollector (req: Q) (acks: ReplyTracker, backoff: Backoff) (
      implicit scheduler: Scheduler, cluster: Cluster) {

    private val fiber = new Fiber (scheduler)
    private val mbx = open (fiber) (receive _)
    private val timer = backoff.iterator

    if (timer.hasNext)
      fiber.delay (timer.next) (_send())

    private def _send(): Unit = fiber.execute {
      if (!acks.quorum) {
        if (timer.hasNext) {
          apply (req) (acks, mbx)
          fiber.delay (timer.next) (_send())
        } else {
          mbx.close()
          timeout()
        }}}

    private def receive (rsp: A, from: Peer) {
      if (!acks.quorum) {
        process (rsp)
        acks += from
        if (timer.hasNext) {
          if (acks.quorum) {
            mbx.close()
            quorum()
          }}}}

    def process (rsp: A)
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

  def open (s: Scheduler) (f: (A, Peer) => Any) (implicit c: Cluster): Mailbox =
    c.open (prsp) (f)
}

object RequestDescriptor {

  def apply [Q, A] (id: MailboxId, preq: Pickler [Q], pans: Pickler [A]): RequestDescriptor [Q, A] =
    new RequestDescriptor (id, preq, pans)
}
