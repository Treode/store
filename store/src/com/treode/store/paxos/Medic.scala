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

import com.treode.async.{Async, Fiber}
import com.treode.store.{BallotNumber, Bytes, TxClock}
import com.treode.store.tier.TierMedic

private class Medic (
    val key: Bytes,
    var default: Option [Bytes],
    var ballot: BallotNumber,
    var proposal: Proposal,
    var chosen: Option [Bytes],
    kit: RecoveryKit) {

  import kit.archive

  def opened (default: Bytes): Unit = synchronized {
    if (this.default.isEmpty)
      this.default = Some (default)
  }

  def granted (ballot: BallotNumber): Unit = synchronized {
    if (this.ballot < ballot)
      this.ballot = ballot
  }

  def accepted (ballot: BallotNumber, value: Bytes): Unit = synchronized {
    if (this.ballot < ballot) {
      this.ballot = ballot
      this.proposal = Some ((ballot, value))
    } else if (proposal.isEmpty) {
      this.proposal = Some ((ballot, value))
    }}

  def reaccepted (ballot: BallotNumber): Unit = synchronized {
    if (this.ballot < ballot) {
      this.ballot = ballot
      if (proposal.isDefined)
        this.proposal = Some ((ballot, proposal.get._2))
    }}

  def closed (chosen: Bytes, gen: Long): Unit = synchronized {
    this.chosen = Some (chosen)
    archive.put (gen, key, TxClock.MaxValue, chosen)
  }

  def close (kit: PaxosKit): Unit = synchronized {
    val a = new Acceptor (key, kit)
    if (chosen.isDefined)
      a.choose (chosen.get)
    else if (default.isDefined)
      a.recover (default.get, ballot, proposal)
    else if (proposal.isDefined)
      a.recover (proposal.get._2, ballot, proposal)
    else
      assert (false, s"Failed to recover paxos instance $key")
    kit.acceptors.recover (key, a)
  }

  override def toString = s"Acceptor.Medic($key, $default, $ballot, $proposal, $chosen)"
}

private object Medic {

  def apply (key: Bytes, default: Option [Bytes], kit: RecoveryKit): Medic =
    new Medic (key, default, BallotNumber.zero, Option.empty, None, kit)
}
