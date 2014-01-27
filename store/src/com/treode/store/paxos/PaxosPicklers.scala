package com.treode.store.paxos

import com.treode.disk.Position
import com.treode.store.StorePicklers

private class PaxosPicklers extends StorePicklers {

  def acceptorStatus = Acceptor.Status.pickler
  def ballotNumber = BallotNumber.pickler
}

private object PaxosPicklers extends PaxosPicklers {

  val position = Position.pickler
  val proposal = option (tuple (ballotNumber, bytes))
}
