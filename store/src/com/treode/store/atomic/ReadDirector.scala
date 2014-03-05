package com.treode.store.atomic

import java.util.concurrent.TimeoutException
import scala.language.postfixOps

import com.treode.async.{Backoff, Callback, Fiber}
import com.treode.async.misc.RichInt
import com.treode.cluster.Peer
import com.treode.store.{ReadOp, TxClock, Value}

private class ReadDirector (
    rt: TxClock,
    ops: Seq [ReadOp],
    kit: AtomicKit,
    private var cb: Callback [Seq [Value]]
) {

  import kit.{atlas, cluster, random, scheduler}

  val readBackoff = Backoff (100, 100, 1 seconds, 7) (random)
  val closedLifetime = 2 seconds

  val fiber = new Fiber (scheduler)
  val mbx = cluster.open (ReadResponse.pickler) (receive _)
  val backoff = readBackoff.iterator
  val acks = atlas.locate (0)
  val gots = atlas.locate (0)
  val vs = Array.fill (ops.size) (Value.empty)

  ReadDeputy.read (rt, ops) (acks, mbx)
  fiber.delay (backoff.next) (timeout())

  private def maybeFinish() {
    if (!acks.quorum) return
    val _cb = cb
    cb = null
    if (gots.quorum) {
      _cb.pass (vs)
    } else {
      _cb.fail (new Exception)
    }}

  private def got (_vs: Seq [Value], from: Peer) {
    if (cb == null) return
    acks += from
    gots += from
    for ((v, i) <- _vs.zipWithIndex)
      if (vs(i).time < v.time)
        vs(i) = v
    maybeFinish()
  }

  private def failed (from: Peer) {
    if (cb == null) return
    acks += from
    maybeFinish()
  }

  def receive (rsp: ReadResponse, from: Peer): Unit = fiber.execute {
    import ReadResponse._
    rsp match {
      case Got (vs) => got (vs, from)
      case Failed => failed (from)
    }}

  def timeout() {
    if (cb == null) return
    if (backoff.hasNext) {
      ReadDeputy.read (rt, ops) (acks, mbx)
      fiber.delay (backoff.next) (timeout())
    } else {
      val _cb = cb
      cb = null
      _cb.fail (new TimeoutException)
    }}}
