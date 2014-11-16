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

import com.treode.cluster.Peer
import com.treode.store.Cohort

/** Tracks hosts that have acknowledged request. Rouses hosts that have not acknowledged the
  * request. For tracking acknowledgements to commit requests.
  */
private class BroadTracker (_rouse: Set [Peer] => Any, var hosts: Set [Peer]) {

  def += (p: Peer): Unit =
    hosts -= p

  def unity: Boolean =
    hosts.isEmpty

  def rouse(): Unit =
    _rouse (hosts)
}

private object BroadTracker {

  def apply (cohorts: Seq [Cohort], kit: AtomicKit) (rouse: Set [Peer] => Any): BroadTracker = {
    import kit.cluster.peer
    val hosts = cohorts.map (_.hosts) .fold (Set.empty) (_ | _) .map (peer (_))
    new BroadTracker (rouse, hosts)
  }}
