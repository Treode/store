package com.treode.store

import java.util.concurrent.ConcurrentHashMap

import com.treode.async.Callback
import com.treode.pickle.PicklerRegistry

package object catalog {

  private [catalog] type Ping = Seq [(CatalogId, Int)]
  private [catalog] type Sync = Seq [(CatalogId, Update)]

  private [catalog] type Proposal = Option [(BallotNumber, Patch)]
  private [catalog] type Learner = Callback [Update]

  private [catalog] def newAcceptorsMap = new ConcurrentHashMap [(CatalogId, Int), Acceptor]
  private [catalog] def newProposersMap = new ConcurrentHashMap [(CatalogId, Int), Proposer]

  private [catalog] val catalogChunkSize = 16
  private [catalog] val catalogHistoryLimit = 16
}
