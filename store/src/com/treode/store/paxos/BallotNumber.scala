package com.treode.store.paxos

import com.treode.cluster.HostId

class BallotNumber private (
    val number: Long,
    val host: HostId) extends Ordered [BallotNumber] {

  /** Prevent preferential treatment of higher numbered hosts. */
  private def ordinal = math.abs (number - host.id)

  def compare (that: BallotNumber): Int = {
    val r = number compareTo that.number
    if (r != 0) r else ordinal compareTo that.ordinal
  }

  override def equals (other: Any): Boolean =
    other match {
      case that: BallotNumber => number == that.number && host == that.host
      case _ => false
    }

  override def hashCode: Int = (number, host).hashCode

  override def toString = f"BallotNumber:$number%X:${host.id}%X"
}

object BallotNumber extends Ordering [BallotNumber] {

  val zero = new BallotNumber (0, 0)

  def apply (number: Long, host: HostId) = new BallotNumber (number, host)

  def compare (x: BallotNumber, y: BallotNumber) = x compare (y)

  val pickler = {
    import PaxosPicklers._
    wrap (ulong, hostId) build {
      v => BallotNumber (v._1, v._2)
    } inspect {
      v => (v.number, v.host)
    }
  }}
