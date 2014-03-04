package com.treode.store

import java.util.concurrent.ConcurrentHashMap

import com.treode.async.Callback
import com.treode.cluster.MailboxId
import com.treode.store.paxos.BallotNumber

package object catalog {

  private [catalog] type Ping = Seq [(MailboxId, Int)]
  private [catalog] type Sync = Seq [(MailboxId, Update)]

  private [catalog] type Proposal = Option [(BallotNumber, Patch)]
  private [catalog] type Learner = Callback [Update]

  private [catalog] def newAcceptorsMap = new ConcurrentHashMap [MailboxId, Acceptor]
  private [catalog] def newProposersMap = new ConcurrentHashMap [MailboxId, Proposer]

  private [catalog] val catalogChunkSize = 16
  private [catalog] val catalogHistoryLimit = 16
}
