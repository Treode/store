package com.treode.store.cluster.paxos

import com.treode.disk.Position
import com.treode.store.StorePicklers

private class PaxosPicklers extends StorePicklers {

  def acceptorStatus = Acceptor.Status.pickle
  def ballotNumber = BallotNumber.pickle
}

private object PaxosPicklers extends PaxosPicklers {

  val position = Position.pickle
  val proposal = option (tuple (ballotNumber, bytes))
}
