package com.treode.store

import com.treode.async.Callback

package object paxos {

  private [paxos] type Learner = Callback [Bytes]
  private [paxos] type Proposal = Option [(BallotNumber, Bytes)]
}
