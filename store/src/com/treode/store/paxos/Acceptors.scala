package com.treode.store.paxos

import com.treode.async.{Async, AsyncImplicits, Latch}
import com.treode.async.misc.materialize
import com.treode.disk.{Disks, ObjectId, PageDescriptor, PageHandler, Position, RecordDescriptor}
import com.treode.store.{Bytes, Cell, Residents, TxClock}
import com.treode.store.tier.{TierDescriptor, TierTable}

import Async.{guard, latch, supply}
import AsyncImplicits._
import PaxosKit.locator

private class Acceptors (kit: PaxosKit) extends PageHandler [Long] {
  import kit.{archive, atlas, cluster, disks}

  val acceptors = newAcceptorsMap

  def get (key: Bytes, time: TxClock): Acceptor = {
    var a0 = acceptors.get ((key, time))
    if (a0 != null)
      return a0
    val a1 = new Acceptor (key, time, kit)
    a1.state = new a1.Opening
    a0 = acceptors.putIfAbsent ((key, time), a1)
    if (a0 != null)
      return a0
    a1
  }

  def remove (key: Bytes, time: TxClock, a: Acceptor): Unit =
    acceptors.remove ((key, time), a)

  def recover (medics: Seq [Medic]): Async [Unit] = {
    for {
      _ <-
        for (m <- medics.latch.unit)
          for (a <- m.close (kit))
            yield acceptors.put ((m.key, m.time), a)
    } yield ()
  }

  def probe (obj: ObjectId, groups: Set [Long]): Async [Set [Long]] =
    supply (archive.probe (groups))

  def compact (obj: ObjectId, groups: Set [Long]): Async [Unit] =
    guard {
      val residents = atlas.residents
      for {
        meta <- archive.compact (groups, residents)
        _ <- Acceptors.checkpoint.record (meta)
      } yield ()
    }

  def checkpoint(): Async [Unit] =
    guard {
      val residents = atlas.residents
      for {
        _ <- latch (
            archive.checkpoint (residents) .flatMap (Acceptors.checkpoint.record (_)),
            materialize (acceptors.values) .latch.unit foreach (_.checkpoint()))
      } yield ()
    }

  def attach () (implicit launch: Disks.Launch) {
    import Acceptor.{choose, propose, query}

    Acceptors.archive.handle (this)

    query.listen { case ((key, time, ballot, default), c) =>
      get (key, time) query (c, ballot, default)
    }

    propose.listen { case ((key, time, ballot, value), c) =>
      get (key, time) propose (c, ballot, value)
    }

    choose.listen { case ((key, time, chosen), c) =>
      get (key, time) choose (chosen)
    }}}

private object Acceptors {

  val checkpoint = {
    import PaxosPicklers._
    RecordDescriptor (0x42A17DC354412E17L, tierMeta)
  }

  val archive = TierDescriptor (0x9F59C4262C8190E8L) { (residents, _, cell) =>
    residents contains (PaxosKit.locator, (cell.key, cell.time))
  }}
