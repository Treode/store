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

package com.treode.store.paxos

import com.treode.async.Async
import com.treode.store.{Bytes, TxClock}

import Async.async
import Proposer.{accept, chosen, grant, refuse}

private class Proposers (kit: PaxosKit) {
  import kit.cluster
  import kit.library.releaser

  val proposers = newProposersMap

  def get (key: Bytes): Proposer = {
    var p0 = proposers.get (key)
    if (p0 != null) return p0
    val p1 = new Proposer (key, kit)
    p0 = proposers.putIfAbsent (key, p1)
    if (p0 != null) return p0
    p1
  }

  def remove (key: Bytes, p: Proposer): Unit =
    proposers.remove (key, p)

  def propose (ballot: Long, key: Bytes, value: Bytes): Async [Bytes] =
    releaser.join {
      async { cb =>
        val p = get (key)
        p.open (ballot, value)
        p.learn (cb)
      }}

  def attach() {

    refuse.listen { case ((key, ballot), c) =>
      get (key) refuse (c, ballot)
    }

    grant.listen { case ((key, ballot, proposal), c) =>
      get (key) grant (c, ballot, proposal)
    }

    accept.listen { case ((key, ballot), c) =>
      get (key) accept (c, ballot)
    }

    chosen.listen { case ((key, v), _) =>
      get (key) chosen (v)
    }}}
