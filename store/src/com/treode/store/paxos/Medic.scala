package com.treode.store.paxos

import com.treode.async.{Async, Fiber}
import com.treode.store.{BallotNumber, Bytes, TxClock}
import com.treode.store.tier.TierMedic

private class Medic (
    val key: Bytes,
    val time: TxClock,
    var default: Option [Bytes],
    var ballot: BallotNumber,
    var proposal: Proposal,
    var chosen: Option [Bytes],
    kit: RecoveryKit) {

  import kit.archive

  def opened (default: Bytes): Unit = synchronized {
    if (this.default.isEmpty)
      this.default = Some (default)
  }

  def promised (ballot: BallotNumber): Unit = synchronized {
    if (this.ballot < ballot)
      this.ballot = ballot
  }

  def accepted (ballot: BallotNumber, value: Bytes): Unit = synchronized {
    if (this.ballot < ballot) {
      this.ballot = ballot
      this.proposal = Some ((ballot, value))
    } else if (proposal.isEmpty) {
      this.proposal = Some ((ballot, value))
    }}

  def reaccepted (ballot: BallotNumber): Unit = synchronized {
    if (this.ballot < ballot) {
      this.ballot = ballot
      if (proposal.isDefined)
        this.proposal = Some ((ballot, proposal.get._2))
    }}

  def closed (chosen: Bytes, gen: Long): Unit = synchronized {
    this.chosen = Some (chosen)
    archive.put (gen, key, time, chosen)
  }

  def close (kit: PaxosKit): Unit = synchronized {
    val a = new Acceptor (key, time, kit)
    if (chosen.isDefined)
      a.state = new a.Closed (chosen.get, 0)
    else if (default.isDefined)
      a.state = new a.Deliberating (default.get, ballot, proposal, Set.empty)
    else if (proposal.isDefined)
      a.state = new a.Deliberating (proposal.get._2, ballot, proposal, Set.empty)
    else
      assert (false, s"Failed to recover paxos instance $key:$time")
    kit.acceptors.recover (key, time, a)
  }

  override def toString = s"Acceptor.Medic($key, $time, $default, $ballot, $proposal, $chosen)"
}

private object Medic {

  def apply (key: Bytes, time: TxClock, default: Option [Bytes], kit: RecoveryKit): Medic =
    new Medic (key, time, default, BallotNumber.zero, Option.empty, None, kit)
}
