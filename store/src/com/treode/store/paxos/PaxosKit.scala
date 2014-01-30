package com.treode.store.paxos

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConversions._
import scala.util.Random

import com.treode.async.{Callback, Fiber, Scheduler, callback, delay, guard}
import com.treode.cluster.{Acknowledgements, Cluster}
import com.treode.cluster.misc.materialize
import com.treode.disk.{Disks, Position, Recovery}
import com.treode.store.{Bytes, StoreConfig}
import com.treode.store.simple.SimpleTable

private class PaxosKit (db: SimpleTable) (implicit val random: Random, val scheduler: Scheduler,
    val cluster: Cluster, val disks: Disks, val config: StoreConfig) extends Paxos {

  val acceptors = new Acceptors (db, this)

  val proposers = new Proposers (this)

  def locate (key: Bytes): Acknowledgements =
    cluster.locate (Bytes.pickler, 0, key)

  def lead (key: Bytes, value: Bytes, cb: Callback [Bytes]): Unit =
    guard (cb) {
      proposers.propose (0, key, value, cb)
    }

  def propose (key: Bytes, value: Bytes, cb: Callback [Bytes]): Unit =
    guard (cb) {
      proposers.propose (random.nextInt (17) + 1, key, value, cb)
  }}

private class PaxosRecovery (implicit val random: Random, val scheduler: Scheduler,
    val cluster: Cluster, val recovery: Recovery, val config: StoreConfig)
