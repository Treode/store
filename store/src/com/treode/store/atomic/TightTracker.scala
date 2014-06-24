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

import com.treode.cluster.{ReplyTracker, Peer}
import com.treode.store.Cohort

private class TightTracker [P] (
    _rouse: (Peer, Seq [P]) => Any,
    val acks: Array [ReplyTracker],
    var msgs: Map [Peer, Seq [P]],
    var idxs: Map [Peer, Seq [Int]]
) {

  def += (p: Peer): Unit = {
    acks foreach (_ += p)
    msgs -= p
  }

  def quorum: Boolean =
    acks forall (_.quorum)

  def rouse(): Unit =
    msgs foreach (_rouse.tupled)
}

private object TightTracker {

  def apply [P] (
      ops: Seq [P],
      cohorts: Seq [Cohort],
      kit: AtomicKit
  ) (
      rouse: (Peer, Seq [P]) => Any
  ): TightTracker [P] = {
    import kit.cluster.peer

    val cidxs = cohorts.zipWithIndex groupBy (_._1)

    var pidxs = Map.empty [Peer, Seq [Int]] .withDefaultValue (Seq.empty)
    for ((cohort, xs) <- cidxs.toSeq; host <- cohort.hosts) {
      val p = peer (host)
      pidxs += p -> (pidxs (p) ++ (xs map (_._2)))
    }

    val pops = pidxs mapValues (_.map (ops (_)))

    val acks = cidxs.keys.map (_.track) .toArray

    new TightTracker (rouse, acks, pops, pidxs)
  }}
