package com.treode.store.paxos

import com.treode.async.{Async, AsyncConversions, Latch}
import com.treode.async.misc.materialize
import com.treode.disk.{Disks, ObjectId, PageDescriptor, PageHandler, Position, RecordDescriptor, RootDescriptor}
import com.treode.store.Bytes
import com.treode.store.tier.{TierDescriptor, TierTable}

import Async.{guard, supply}
import AsyncConversions._

private class Acceptors (kit: PaxosKit) extends PageHandler [Long] {
  import kit.{archive, cluster, disks}

  val acceptors = newAcceptorsMap

  def get (key: Bytes): Acceptor = {
    var a0 = acceptors.get (key)
    if (a0 != null)
      return a0
    val a1 = new Acceptor (key, kit)
    a1.state = new a1.Opening
    a0 = acceptors.putIfAbsent (key, a1)
    if (a0 != null)
      return a0
    a1
  }

  def remove (key: Bytes, a: Acceptor): Unit =
    acceptors.remove (key, a)

  def recover (medics: Seq [Medic]): Async [Unit] = {
    for {
      _ <- medics.latch.unit { m =>
        for (a <- m.close (kit))
          yield acceptors.put (m.key, a)
      }
    } yield ()
  }

  def probe (obj: ObjectId, groups: Set [Long]): Async [Set [Long]] =
    supply (archive.probe (groups))

  def compact (obj: ObjectId, groups: Set [Long]): Async [Unit] =
    guard {
      for {
        meta <- archive.compact (groups)
        _ <- Acceptors.checkpoint.record (meta)
      } yield ()
    }

  def checkpoint(): Async [Unit] =
    guard {
      for {
        _ <- Latch.pair (
            archive.checkpoint() .flatMap (Acceptors.checkpoint.record (_)),
            materialize (acceptors.values) .latch.unit (_.checkpoint()))
      } yield ()
    }

  def attach () (implicit launch: Disks.Launch) {
    import Acceptor.{choose, propose, query}

    Acceptors.root.checkpoint (checkpoint())

    Acceptors.archive.handle (this)

    query.listen { case ((key, ballot, default), c) =>
      get (key) query (c, ballot, default)
    }

    propose.listen { case ((key, ballot, value), c) =>
      get (key) propose (c, ballot, value)
    }

    choose.listen { case ((key, chosen), c) =>
      get (key) choose (chosen)
    }}}

private object Acceptors {

  val root = {
    import PaxosPicklers._
    RootDescriptor (0x4BF7F275BBE8086EL, unit)
  }

  val checkpoint = {
    import PaxosPicklers._
    RecordDescriptor (0x42A17DC354412E17L, tierMeta)
  }

  val archive = {
    import PaxosPicklers._
    TierDescriptor (0x9F59C4262C8190E8L, bytes, const (true))
  }}
