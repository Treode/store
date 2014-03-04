package com.treode.store.paxos

import com.treode.store.StorePicklers

private class PaxosPicklers extends StorePicklers {

  def activeStatus = Acceptor.ActiveStatus.pickler

  lazy val proposal = option (tuple (ballotNumber, bytes))
}

private object PaxosPicklers extends PaxosPicklers
