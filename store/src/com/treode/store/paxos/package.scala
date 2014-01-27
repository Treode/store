package com.treode.store

import java.util.concurrent.ConcurrentHashMap
import com.treode.async.Callback

package object paxos {

  private [paxos] type AcceptorsMap = ConcurrentHashMap [Bytes, Acceptor]
  private [paxos] type MedicsMap = ConcurrentHashMap [Bytes, Medic]
  private [paxos] type Learner = Callback [Bytes]
  private [paxos] type Proposal = Option [(BallotNumber, Bytes)]
  private [paxos] type ProposersMap = ConcurrentHashMap [Bytes, Proposer]

  private [paxos] def newAcceptorsMap = new ConcurrentHashMap [Bytes, Acceptor]
  private [paxos] def newMedicsMap = new ConcurrentHashMap [Bytes, Medic]
  private [paxos] def newProposersMap = new ConcurrentHashMap [Bytes, Proposer]
}
