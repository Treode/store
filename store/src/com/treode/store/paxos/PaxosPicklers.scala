package com.treode.store.paxos

import com.treode.store.StorePicklers

private class PaxosPicklers extends StorePicklers {

  lazy val proposal = option (tuple (ballotNumber, bytes))
}

private object PaxosPicklers extends PaxosPicklers
