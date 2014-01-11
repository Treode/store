package com.treode.store.disk2

import java.nio.file.Path
import com.treode.async.Callback
import com.treode.pickle.Picklers
import com.treode.store.Bytes
import com.treode.store.cluster.paxos.BallotNumber

object LogEntry {

  sealed abstract class Body

  case object End extends Body
  case class Continue (seg: Int) extends Body
  case class PaxosOpen (key: Bytes, default: Bytes) extends Body
  case class PaxosPromise (key: Bytes, ballot: BallotNumber) extends Body
  case class PaxosAccept (key: Bytes, ballot: BallotNumber, value: Bytes) extends Body
  case class PaxosReaccept (key: Bytes, ballot: BallotNumber) extends Body
  case class PaxosClose (key: Bytes, value: Bytes) extends Body
  case class Update (s: String) extends Body

  object Body {

    val pickle = {
      import Picklers._
      val ballot = BallotNumber.pickle
      val bytes = Bytes.pickle
      tagged [Body] (
          0x00405750E4FE92DAL -> const (End),
          0x1 -> wrap1 (int) (Continue.apply _) (_.seg),
          0x2 -> wrap2 (bytes, bytes) (PaxosOpen.apply _) (v => (v.key, v.default)),
          0x3 -> wrap2 (bytes, ballot) (PaxosPromise.apply _) (v => (v.key, v.ballot)),
          0x4 -> wrap3 (bytes, ballot, bytes) (PaxosAccept.apply _) (v => (v.key, v.ballot, v.value)),
          0x5 -> wrap2 (bytes, ballot) (PaxosReaccept.apply _) (v => (v.key, v.ballot)),
          0x6 -> wrap2 (bytes, bytes) (PaxosClose.apply _) (v => (v.key, v.value)),
          0x7 -> wrap1 (string) (Update.apply _) (_.s))
    }}

  case class Header (time: Long, len: Int)

  object Header {

    val ByteSize = 12

    val pickle = {
      import Picklers._
      wrap2 (fixedLong, fixedInt) (Header.apply _) (v => (v.time, v.len))
    }}

  case class Envelope (hdr: Header, body: Body) extends Ordered [Envelope] {

    def compare (that: Envelope): Int =
      hdr.time compare that.hdr.time
  }

  object Envelope extends Ordering [Envelope] {

    def compare (x: Envelope, y: Envelope): Int =
      x compare y
  }

  case class Pending (body: Body, time: Long, cb: Callback [Unit])

  val SegmentTrailerBytes = Header.ByteSize + 8
}
