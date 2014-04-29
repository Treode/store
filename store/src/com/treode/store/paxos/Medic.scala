package com.treode.store.paxos

import com.treode.async.{Async, Fiber}
import com.treode.store.{BallotNumber, Bytes, TxClock}
import com.treode.store.tier.TierMedic

private class Medic (
    val key: Bytes,
    val time: TxClock,
    val default: Bytes,
    var ballot: BallotNumber,
    var proposal: Proposal,
    var chosen: Option [Bytes],
    kit: RecoveryKit) {

  import kit.{archive, scheduler}

  val fiber = new Fiber (scheduler)

  def promised (ballot: BallotNumber): Unit = fiber.execute {
    if (this.ballot < ballot)
      this.ballot = ballot
  }

  def accepted (ballot: BallotNumber, value: Bytes): Unit = fiber.execute {
    if (this.ballot < ballot) {
      this.ballot = ballot
      this.proposal = Some ((ballot, value))
    }}

  def reaccepted (ballot: BallotNumber): Unit = fiber.execute {
    val value = proposal.get._2
    if (this.ballot < ballot) {
      this.ballot = ballot
      this.proposal = Some ((ballot, value))
    }}

  def closed (chosen: Bytes, gen: Long): Unit = fiber.execute {
    this.chosen = Some (chosen)
    archive.put (gen, key, time, chosen)
  }

  def close (kit: PaxosKit): Async [Acceptor] = fiber.supply {
    val a = new Acceptor (key, time, kit)
    if (chosen.isDefined)
      a.state = new a.Closed (chosen.get, 0)
    else
      a.state = new a.Deliberating (default, ballot, proposal, Set.empty)
    a
  }

  override def toString = s"Acceptor.Medic($key, $default, $proposal, $chosen)"
}

private object Medic {

  def apply (key: Bytes, time: TxClock, default: Bytes, kit: RecoveryKit): Medic =
    new Medic (key, time, default, BallotNumber.zero, Option.empty, None, kit)
}
