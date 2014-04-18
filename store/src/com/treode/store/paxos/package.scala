package com.treode.store

import java.util.concurrent.ConcurrentHashMap
import com.treode.async.Callback

package object paxos {

  private [paxos] type AcceptorsMap = ConcurrentHashMap [(Bytes, TxClock), Acceptor]
  private [paxos] type MedicsMap = ConcurrentHashMap [(Bytes, TxClock), Medic]
  private [paxos] type Learner = Callback [Bytes]
  private [paxos] type Proposal = Option [(BallotNumber, Bytes)]
  private [paxos] type ProposersMap = ConcurrentHashMap [(Bytes, TxClock), Proposer]

  private [paxos] def newAcceptorsMap = new ConcurrentHashMap [(Bytes, TxClock), Acceptor]
  private [paxos] def newMedicsMap = new ConcurrentHashMap [(Bytes, TxClock), Medic]
  private [paxos] def newProposersMap = new ConcurrentHashMap [(Bytes, TxClock), Proposer]
}
