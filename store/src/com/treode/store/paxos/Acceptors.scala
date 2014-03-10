package com.treode.store.paxos

import com.treode.async.{Async, AsyncConversions, Latch}
import com.treode.async.misc.materialize
import com.treode.disk.{PageDescriptor, Position, RootDescriptor}
import com.treode.store.Bytes
import com.treode.store.tier.{TierDescriptor, TierTable}

import Acceptors.{Root, active}
import Async.guard
import AsyncConversions._

private class Acceptors (kit: PaxosKit) {
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

  def checkpoint(): Async [Root] =
    guard {
      val as = materialize (acceptors.values)
      for {
        (ss, _archive) <- Latch.pair (
            as.latch.seq (_.checkpoint()),
            archive.checkpoint())
        _active <- active.write (0, 0, ss.flatten)
      } yield new Root (_active, _archive)
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

  class Root (val active: Position, val archive: TierTable.Meta)

  object Root {

    val pickler = {
      import PaxosPicklers._
      wrap (pos, tierMeta)
      .build (v => new Root (v._1, v._2))
      .inspect (v => (v.active, v.archive))
    }}

  val root = {
    import PaxosPicklers._
    RootDescriptor (0x4BF7F275BBE8086EL, Root.pickler)
  }

  val active = {
    import PaxosPicklers._
    PageDescriptor (0x6D5C9C6CA7E4ACC5L, const (0), seq (activeStatus))
  }

  val archive = {
    import PaxosPicklers._
    TierDescriptor (0x9F59C4262C8190E8L, bytes, const (true))
  }}
