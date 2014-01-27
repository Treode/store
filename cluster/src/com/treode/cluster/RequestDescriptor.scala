package com.treode.cluster

import com.treode.async.{Fiber, Scheduler}
import com.treode.pickle.{Pickler, Picklers}
import com.treode.cluster.misc.BackoffTimer

class RequestDescriptor [Req, Rsp] (id: MailboxId, preq: Pickler [Req], prsp: Pickler [Rsp]) {

  type Mailbox = EphemeralMailbox [Rsp]
  type Mediator = RequestMediator [Rsp]

  abstract class QuorumCollector (req: Req) (acks: Acknowledgements, backoff: BackoffTimer) (
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

    private def receive (f: Rsp => Any) {
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

    def process (f: Rsp => Any) {
      receive (f)
      _send()
    }

    def quorum()
    def timeout()
  }

  private val _preq = {
    import Picklers._
    tuple (MailboxId.pickler, preq)
  }

  def listen (f: (Req, Mediator) => Any) (implicit c: Cluster): Unit =
    c.listen (new MessageDescriptor (id, _preq)) { case ((mbx, req), c) =>
      f (req, new RequestMediator (prsp, mbx, c))
    }

  def apply (req: Req) = RequestSender [Req, Rsp] (id, _preq, req)

  def open (s: Scheduler) (implicit c: Cluster): Mailbox =
    c.open (prsp, s)
}
