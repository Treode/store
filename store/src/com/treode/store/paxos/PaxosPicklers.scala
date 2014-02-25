package com.treode.store.paxos

import com.treode.disk.Position
import com.treode.store.StorePicklers

private class PaxosPicklers extends StorePicklers {

  def activeStatus = Acceptor.ActiveStatus.pickler
  def ballotNumber = BallotNumber.pickler
  def position = Position.pickler

  lazy val proposal = option (tuple (ballotNumber, bytes))
}

private object PaxosPicklers extends PaxosPicklers
