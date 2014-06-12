package com.treode.store.paxos

import com.treode.async.{Async, Latch}
import com.treode.async.implicits._
import com.treode.async.misc.materialize
import com.treode.disk.{Disk, ObjectId, PageDescriptor, PageHandler, Position, RecordDescriptor}
import com.treode.store.{Bytes, Cell, Residents, TxClock}
import com.treode.store.tier.{TierDescriptor, TierTable}

import Async.{guard, latch, supply}

private class Acceptors (kit: PaxosKit) extends PageHandler [Long] {
  import kit.{archive, cluster, disk, library}
  import kit.library.atlas

  val acceptors = newAcceptorsMap

  def get (key: Bytes, time: TxClock): Acceptor = {
    var a0 = acceptors.get ((key, time))
    if (a0 != null)
      return a0
    val a1 = new Acceptor (key, time, kit)
    a0 = acceptors.putIfAbsent ((key, time), a1)
    if (a0 != null) {
      a1.dispose()
      return a0
    }
    a1
  }

  def recover (key: Bytes, time: TxClock, a: Acceptor): Unit =
    acceptors.put ((key, time), a)

  def remove (key: Bytes, time: TxClock, a: Acceptor): Unit =
    acceptors.remove ((key, time), a)

  def probe (obj: ObjectId, groups: Set [Long]): Async [Set [Long]] =
    guard {
      archive.probe (groups)
    }

  def compact (obj: ObjectId, groups: Set [Long]): Async [Unit] =
    guard {
      for {
        meta <- archive.compact (groups, library.residents)
        _ <- Acceptors.checkpoint.record (meta)
      } yield ()
    }

  def checkpoint(): Async [Unit] =
    guard {
      for {
        _ <- materialize (acceptors.values) .latch.unit foreach (_.checkpoint())
        meta <- archive.checkpoint (library.residents)
        _ <- Acceptors.checkpoint.record (meta)

      } yield ()
    }

  def attach () (implicit launch: Disk.Launch) {
    import Acceptor.{choose, propose, query}

    launch.checkpoint (checkpoint())

    Acceptors.archive.handle (this)

    query.listen { case ((version, key, time, ballot, default), c) =>
      if (atlas.version - 1 <= version && version <= atlas.version + 1)
        get (key, time) query (c, ballot, default)
    }

    propose.listen { case ((version, key, time, ballot, value), c) =>
      if (atlas.version - 1 <= version && version <= atlas.version + 1)
        get (key, time) propose (c, ballot, value)
    }

    choose.listen { case ((key, time, chosen), c) =>
      get (key, time) choose (chosen)
    }}}

private object Acceptors {

  val receive = {
    import PaxosPicklers._
    RecordDescriptor (0x4DCE11AA, tuple (ulong, seq (cell)))
  }

  val checkpoint = {
    import PaxosPicklers._
    RecordDescriptor (0x42A17DC354412E17L, tierMeta)
  }

  val archive = TierDescriptor (0x9F59C4262C8190E8L) { (residents, _, cell) =>
    resident (residents, cell.key, cell.time)
  }}
