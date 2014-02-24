package com.treode.store.atomic

import java.util.concurrent.TimeoutException
import scala.language.postfixOps

import com.treode.async.{Callback, Fiber}
import com.treode.cluster.Peer
import com.treode.cluster.misc.{BackoffTimer, RichInt}
import com.treode.store.{ReadOp, TxClock, Value}

private class ReadDirector (
    rt: TxClock,
    ops: Seq [ReadOp],
    kit: AtomicKit,
    private var cb: Callback [Seq [Value]]) {

  import kit.{cluster, random, scheduler}

  val readBackoff = BackoffTimer (100, 100, 1 seconds, 7) (random)
  val closedLifetime = 2 seconds

  val fiber = new Fiber (scheduler)
  val mbx = cluster.open (ReadResponse.pickler, fiber)

  val backoff = readBackoff.iterator
  val acks = cluster.locate (0)
  val gots = cluster.locate (0)
  val vs = Array.fill (ops.size) (Value.empty)

  private def maybeFinish() {
    if (!acks.quorum) return
    val _cb = cb
    cb = null
    if (gots.quorum) {
      _cb.pass (vs)
    } else {
      _cb.fail (new Exception)
    }}

  def got (_vs: Seq [Value], from: Peer) {
    if (cb == null) return
    acks += from
    gots += from
    for ((v, i) <- _vs.zipWithIndex)
      if (vs(i).time < v.time)
        vs(i) = v
    maybeFinish()
  }

  def failed (from: Peer) {
    if (cb == null) return
    acks += from
    maybeFinish()
  }

  def timeout() {
    if (cb == null) return
    if (backoff.hasNext) {
      ReadDeputy.read (rt, ops) (acks, mbx)
      fiber.delay (backoff.next) (timeout())
    } else {
      val _cb = cb
      cb = null
      _cb.fail (new TimeoutException)
    }}

  ReadDeputy.read (rt, ops) (acks, mbx)
  fiber.delay (backoff.next) (timeout())
  mbx.whilst (cb != null) { (msg, from) =>
    import ReadResponse._
    msg match {
      case Got (vs) => got (vs, from)
      case Failed => failed (from)
    }}}
