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

package com.treode.store

import java.util.concurrent.ConcurrentHashMap
import com.treode.async.Callback
import com.treode.cluster.ReplyTracker

package object paxos {

  private [paxos] type AcceptorsMap = ConcurrentHashMap [Bytes, Acceptor]
  private [paxos] type MedicsMap = ConcurrentHashMap [Bytes, Medic]
  private [paxos] type Learner = Callback [Bytes]
  private [paxos] type Proposal = Option [(BallotNumber, Bytes)]
  private [paxos] type ProposersMap = ConcurrentHashMap [Bytes, Proposer]

  private [paxos] def newAcceptorsMap = new ConcurrentHashMap [Bytes, Acceptor]
  private [paxos] def newMedicsMap = new ConcurrentHashMap [Bytes, Medic]
  private [paxos] def newProposersMap = new ConcurrentHashMap [Bytes, Proposer]

  private val locator = {
    import PaxosPicklers._
    tuple (bytes, txClock)
  }

  private [paxos] def resident (residents: Residents, key: Bytes): Boolean =
    residents.contains (key.murmur32)

  private [paxos] def locate (atlas: Atlas, key: Bytes): Cohort =
    atlas.locate (key.murmur32)

  private [paxos] def place (atlas: Atlas, key: Bytes): Int =
    atlas.place (key.murmur32)

  private [paxos] def track (atlas: Atlas, key: Bytes): ReplyTracker =
    atlas.locate (key.murmur32) .track
}
