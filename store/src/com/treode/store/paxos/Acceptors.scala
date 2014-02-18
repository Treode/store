package com.treode.store.paxos

import com.treode.async.{Async, AsyncConversions, Callback, Latch, Scheduler}
import com.treode.cluster.Cluster
import com.treode.cluster.misc.materialize
import com.treode.disk.{Disks, Position, RecordDescriptor}
import com.treode.store.{Bytes, StoreConfig}
import com.treode.store.tier.{TierMedic, TierTable}

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

  def checkpoint(): Async [Position] =
    guard {
      import Acceptor.{Status, statii}
      val as = materialize (acceptors.values)
      for {
        ss <- as.latch.seq (_.checkpoint())
        pos <- statii.write (0, ss)
      } yield pos
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

  val checkpoint = {
    import PaxosPicklers._
    RecordDescriptor (0x8B97BEF0, tierMeta)
  }

  def attach (kit: PaxosRecovery, cb: Callback [Paxos]) {
    import kit.{cluster, config, random, recovery, scheduler}

    val db = TierMedic (Acceptor.db)
    val medics = newMedicsMap

    def openByStatus (status: Status) {
      val m1 = Medic (status, db, kit)
      val m0 = medics.putIfAbsent (m1.key, m1)
      require (m0 == null, "Already recovering paxos instance ${m1.key}.")
    }

    def openWithDefault (key: Bytes, default: Bytes) {
      var m0 = medics.get (key)
      if (m0 != null)
        return
      val m1 = Medic (key, default, db, kit)
      m0 = medics.putIfAbsent (key, m1)
      if (m0 != null)
        return
    }

    def get (key: Bytes): Medic = {
      val m = medics.get (key)
      require (m != null, s"Exepcted to be recovering paxos instance $key.")
      m
    }

    root.reload { pos => implicit reloader =>
      for (ss <- statii.read (reloader, pos))
        yield (ss foreach openByStatus)
    }

    checkpoint.replay { case meta =>
      db.checkpoint (meta)
    }

    open.replay { case (key, default) =>
      openWithDefault (key, default)
    }

    promise.replay { case (key, ballot) =>
      get (key) promised (ballot)
    }

    accept.replay { case (key, ballot, value) =>
      get (key) accepted (ballot, value)
    }

    reaccept.replay { case (key, ballot) =>
      get (key) reaccepted (ballot)
    }

    close.replay { case (key, chosen, gen) =>
      get (key) closed (chosen, gen)
    }

    recovery.launch { implicit launcher =>
      import launcher.disks

      val kit = new PaxosKit (db.close())
      import kit.{acceptors, proposers}

      root.checkpoint (acceptors.checkpoint())

      for {
        _ <- acceptors.recover (materialize (medics.values))
      } yield {
        acceptors.attach()
        proposers.attach()
        cb.pass (kit)
      }}}}
