package com.treode.store.atomic

import scala.language.postfixOps
import scala.util.{Failure, Success}

import com.treode.async.{Async, Backoff, Callback, Fiber}
import com.treode.async.implicits._
import com.treode.async.misc.RichInt
import com.treode.cluster.{HostId, Peer}
import com.treode.store.{ReadOp, TxClock, Value}

import Async.async

private class ReadDirector (
    rt: TxClock,
    ops: Seq [ReadOp],
    kit: AtomicKit,
    cb: Callback [Seq [Value]]
) {

  import kit.{cluster, random, scheduler}
  import kit.config.readBackoff

  val fiber = new Fiber
  val vs = Array.fill (ops.size) (Value.empty)

  val cohorts = ops map (kit.locate (_))

  val broker = TightTracker (ops, cohorts, kit) { (host, ops) =>
    ReadDeputy.read (rt, ops) (host, port)
  }

  val port = ReadDeputy.read.open { (rsp, from) =>
    fiber.execute {
      rsp match {
        case Success (vs) => got (vs, from)
        case Failure (t) => ()
      }}}

  val timer = cb.ensure {
    port.close()
  } .timeout (fiber, readBackoff) {
    broker.rouse()
  }
  broker.rouse()

  def got (_vs: Seq [Value], from: Peer) {
    if (timer.invoked) return
    broker += from
    for ((v, i) <- _vs.zip (broker.idxs (from)))
      if (vs (i) .time < v.time)
        vs (i) = v
    if (broker.quorum)
      timer.pass (vs)
  }}

private object ReadDirector {

  def read (rt: TxClock, ops: Seq [ReadOp], kit: AtomicKit): Async [Seq [Value]] =
    async (new ReadDirector (rt, ops, kit, _))
}
