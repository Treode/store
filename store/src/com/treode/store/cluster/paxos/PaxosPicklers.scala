package com.treode.store.cluster.paxos

import com.treode.store.StorePicklers

private class PaxosPicklers extends StorePicklers {

  def ballotNumber = BallotNumber.pickle
  def paxosStatus = PaxosStatus.pickle
}

private object PaxosPicklers extends PaxosPicklers {

  val proposal = option (tuple (ballotNumber, bytes))
}
