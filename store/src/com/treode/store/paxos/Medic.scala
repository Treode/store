package com.treode.store.paxos

import com.treode.async.{Callback, Fiber}
import com.treode.store.Bytes
import com.treode.store.simple.SimpleMedic

import Acceptor.Status

private class Medic (
    val key: Bytes,
    val default: Bytes,
    var ballot: BallotNumber,
    var proposal: Proposal,
    var chosen: Option [Bytes],
    db: SimpleMedic,
    kit: PaxosKit) {

  import kit.scheduler

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
    db.put (gen, key, chosen)
  }

  def close (acceptors: Acceptors, cb: Callback [Acceptor]): Unit = fiber.execute {
    val a = new Acceptor (key, acceptors, kit)
    if (chosen.isDefined)
      a.state = new a.Closed (chosen.get)
    else
      a.state = new a.Deliberating (default, ballot, proposal, Set.empty)
    cb (a)
  }

  override def toString = s"Acceptor.Medic($key, $default, $proposal, $chosen)"
}

private object Medic {

  def apply (status: Status, db: SimpleMedic, kit: PaxosKit): Medic = {
    status match {
      case Status.Restoring (key, default) =>
        new Medic (key, default, BallotNumber.zero, Option.empty, None, db, kit)
      case Status.Deliberating (key, default, ballot, proposal) =>
        new Medic (key, default, ballot, proposal, None, db, kit)
      case Status.Closed (key, chosen) =>
        new Medic (key, chosen, BallotNumber.zero, Option.empty, Some (chosen), db, kit)
    }}

  def apply (key: Bytes, default: Bytes, db: SimpleMedic, kit: PaxosKit): Medic =
    new Medic (key, default, BallotNumber.zero, Option.empty, None, db, kit)
}
