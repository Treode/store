/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.treode.store.atomic

import java.util.concurrent.{TimeoutException => JTimeoutException}
import scala.language.postfixOps
import scala.util.{Failure, Success}

import com.treode.async.{Async, Backoff, Callback, Fiber}
import com.treode.async.implicits._
import com.treode.async.misc.RichInt
import com.treode.cluster.{HostId, Peer}
import com.treode.store.{ReadOp, TimeoutException, TxClock, Value}

import Async.async

private class ReadDirector private (
    rt: TxClock,
    ops: Seq [ReadOp],
    kit: AtomicKit,
    cb: Callback [Seq [Value]]
) {

  import kit.{cluster, library, random, scheduler}
  import kit.config.readBackoff

  val fiber = new Fiber
  val vs = Array.fill (ops.size) (Value.empty)
  val atlas = library.atlas
  val cohorts = ops map (op => locate (atlas, op.table, op.key))

  val port = ReadDeputy.read.open { (rsp, from) =>
    fiber.execute {
      rsp match {
        case Success (vs) => got (vs, from)
        case Failure (t) => ()
      }}}

  val broker = TightTracker (ops, cohorts, kit) { (host, ops) =>
    ReadDeputy.read (atlas.version, rt, ops) (host, port)
  }

  val timer = cb.ensure {
    port.close()
  } .rescue {
    case t: JTimeoutException => Failure (new TimeoutException)
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

  def read (rt: TxClock, ops: Seq [ReadOp], kit: AtomicKit): Async [Seq [Value]] = {
    require (rt < TxClock.now + TxClock.MaxSkew)
    async (new ReadDirector (rt, ops, kit, _))
  }}
