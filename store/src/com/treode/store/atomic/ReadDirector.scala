package com.treode.store.atomic

import scala.language.postfixOps

import com.treode.async.{Async, Backoff, Callback, Fiber, TimeoutCallback}
import com.treode.async.misc.RichInt
import com.treode.cluster.Peer
import com.treode.store.{ReadOp, TxClock, Value}

import ReadDeputy.read

private class ReadDirector (
    rt: TxClock,
    ops: Seq [ReadOp],
    kit: AtomicKit,
    cb: Callback [Seq [Value]]
) {

  import kit.{atlas, cluster, random, scheduler}
  import kit.config.readBackoff

  val fiber = new Fiber (scheduler)
  val acks = atlas.locate (0)
  val gots = atlas.locate (0)
  val vs = Array.fill (ops.size) (Value.empty)

  val port = read.open { (rsp, from) =>
    fiber.execute {
      import ReadResponse._
      rsp match {
        case Got (vs) => got (vs, from)
        case Failed => failed (from)
      }}}

  val timer = cb.leave {
    port.close()
  } .timeout (fiber, readBackoff) {
    read (rt, ops) (acks, port)
  }

  private def maybeFinish() {
    if (!acks.quorum) return
    if (gots.quorum) {
      timer.pass (vs)
    } else {
      timer.fail (new Exception)
    }}

  private def got (_vs: Seq [Value], from: Peer) {
    if (timer.invoked) return
    acks += from
    gots += from
    for ((v, i) <- _vs.zipWithIndex)
      if (vs(i).time < v.time)
        vs(i) = v
    maybeFinish()
  }

  private def failed (from: Peer) {
    if (timer.invoked) return
    acks += from
    maybeFinish()
  }}
