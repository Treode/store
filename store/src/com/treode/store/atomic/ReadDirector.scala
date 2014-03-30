package com.treode.store.atomic

import scala.language.postfixOps

import com.treode.async.{Async, AsyncImplicits, Backoff, Callback, Fiber}
import com.treode.async.misc.RichInt
import com.treode.cluster.{HostId, Peer}
import com.treode.store.{DeputyException, ReadOp, TxClock, Value}

import AsyncImplicits._

private class ReadDirector (
    rt: TxClock,
    ops: Seq [ReadOp],
    kit: AtomicKit,
    cb: Callback [Seq [Value]]
) {

  import kit.{cluster, random, scheduler}
  import kit.config.readBackoff

  val fiber = new Fiber (scheduler)
  val vs = Array.fill (ops.size) (Value.empty)

  val cohorts = ops map (kit.locate (_))

  val broker = TightTracker (ops, cohorts, kit) { (host, ops) =>
    ReadDeputy.read (rt, ops) (host, port)
  }

  val port = ReadDeputy.read.open { (rsp, from) =>
    fiber.execute {
      import ReadResponse._
      rsp match {
        case Got (vs) => got (vs, from)
        case Failed => failed (from)
      }}}

  val timer = cb.ensure {
    port.close()
  } .timeout (fiber, readBackoff) {
    broker.rouse()
  }

  def got (_vs: Seq [Value], from: Peer) {
    if (timer.invoked) return
    broker += from
    for ((v, i) <- _vs.zip (broker.idxs (from)))
      if (vs (i) .time < v.time)
        vs (i) = v
    if (broker.quorum)
      timer.pass (vs)
  }

  def failed (from: Peer) {
    if (timer.invoked) return
    timer.fail (new DeputyException)
  }}
