package com.treode.store.cluster

import com.treode.async.Callback
import com.treode.store.Bytes

package object paxos {

  private [paxos] type Learner = Callback [Bytes]
  private [paxos] type Proposal = Option [(BallotNumber, Bytes)]
}
