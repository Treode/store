package com.treode.store.paxos

import com.treode.async.{Async, AsyncConversions}
import com.treode.cluster.misc.materialize
import com.treode.disk.{PageDescriptor, Position, RootDescriptor}
import com.treode.store.Bytes
import com.treode.store.tier.{TierDescriptor, TierTable}

import Acceptors.{Root, statii}
import Async.guard
import AsyncConversions._

private class Acceptors (val db: TierTable, kit: PaxosKit) {
  import kit.{cluster, disks}

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

  def checkpoint(): Async [Root] =
    guard {
      val as = materialize (acceptors.values)
      for {
        ss <- as.latch.seq (_.checkpoint())
        _statii <- statii.write (0, ss)
        _db <- db.checkpoint()
      } yield new Root (_statii, _db)
    }

  def attach() {
    import Acceptor.{choose, propose, query}

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
  import Acceptor._

  class Root (
      private [paxos] val statii: Position,
      private [paxos] val db: TierTable.Meta)

  object Root {

    val pickler = {
      import PaxosPicklers._
      wrap (pos, tierMeta)
      .build (v => new Root (v._1, v._2))
      .inspect (v => (v.statii, v.db))
    }}

  val root = {
    import PaxosPicklers._
    RootDescriptor (0xBFD4F3D3, Root.pickler)
  }

  val statii = {
    import PaxosPicklers._
    PageDescriptor (0x7C71E2AF, const (0), seq (acceptorStatus))
  }

  val db = {
    import PaxosPicklers._
    TierDescriptor (0xDD683792, bytes, const (true))
  }}
