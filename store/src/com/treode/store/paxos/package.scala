package com.treode.store

import java.util.concurrent.ConcurrentHashMap
import com.treode.async.Callback
import com.treode.cluster.ReplyTracker

package object paxos {

  private [paxos] type AcceptorsMap = ConcurrentHashMap [(Bytes, TxClock), Acceptor]
  private [paxos] type MedicsMap = ConcurrentHashMap [(Bytes, TxClock), Medic]
  private [paxos] type Learner = Callback [Bytes]
  private [paxos] type Proposal = Option [(BallotNumber, Bytes)]
  private [paxos] type ProposersMap = ConcurrentHashMap [(Bytes, TxClock), Proposer]

  private [paxos] def newAcceptorsMap = new ConcurrentHashMap [(Bytes, TxClock), Acceptor]
  private [paxos] def newMedicsMap = new ConcurrentHashMap [(Bytes, TxClock), Medic]
  private [paxos] def newProposersMap = new ConcurrentHashMap [(Bytes, TxClock), Proposer]

  private val locator = {
    import PaxosPicklers._
    tuple (bytes, txClock)
  }

  private [paxos] def resident (residents: Residents, key: Bytes, time: TxClock): Boolean =
    residents.contains (locator, (key, time))

  private [paxos] def locate (atlas: Atlas, key: Bytes, time: TxClock): Cohort =
    atlas.locate (locator, (key, time))

  private [paxos] def place (atlas: Atlas, key: Bytes, time: TxClock): Int =
    atlas.place (locator, (key, time))

  private [paxos] def track (atlas: Atlas, key: Bytes, time: TxClock): ReplyTracker =
    atlas.locate (locator, (key, time)) .track
}
