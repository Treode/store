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
          0x1 -> wrap (int)
              .build (Continue.apply _)
              .inspect (_.seg),
          0x2 -> wrap (bytes, bytes)
              .build ((PaxosOpen.apply _).tupled)
              .inspect (v => (v.key, v.default)),
          0x3 -> wrap (bytes, ballot)
              .build ((PaxosPromise.apply _).tupled)
              .inspect (v => (v.key, v.ballot)),
          0x4 -> wrap (bytes, ballot, bytes)
              .build ((PaxosAccept.apply _).tupled)
              .inspect (v => (v.key, v.ballot, v.value)),
          0x5 -> wrap (bytes, ballot)
              .build ((PaxosReaccept.apply _).tupled)
              .inspect (v => (v.key, v.ballot)),
          0x6 -> wrap (bytes, bytes)
              .build ((PaxosClose.apply _).tupled)
              .inspect (v => (v.key, v.value)),
          0x7 -> wrap (string)
              .build (Update.apply _)
              .inspect (_.s))
    }}

  case class Header (time: Long, len: Int)

  object Header {

    val ByteSize = 12

    val pickle = {
      import Picklers._
      wrap (fixedLong, fixedInt)
      .build ((Header.apply _).tupled)
      .inspect (v => (v.time, v.len))
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
