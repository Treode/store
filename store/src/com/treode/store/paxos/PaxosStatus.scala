package com.treode.store.paxos

import com.treode.store.Bytes
import com.treode.pickle.Pickler

private sealed abstract class PaxosStatus

private object PaxosStatus {

  case class Deliberating (v: Bytes, n: BallotNumber, p: Proposal) extends PaxosStatus
  case class Closed (v: Bytes) extends PaxosStatus

  val pickle  = {
    import PaxosPicklers._
    tagged [PaxosStatus] (
        0x1 -> wrap3 (bytes, ballotNumber, proposal) (Deliberating.apply _)  (v => (v.v, v.n, v.p)),
        0x2 -> wrap1 (bytes) (Closed.apply _) (_.v))
  }}
