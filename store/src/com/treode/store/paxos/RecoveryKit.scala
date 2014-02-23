package com.treode.store.paxos

import scala.util.Random

import com.treode.async.Scheduler
import com.treode.cluster.Cluster
import com.treode.disk.Disks
import com.treode.store.StoreConfig

private class RecoveryKit (implicit
    val random: Random,
    val scheduler: Scheduler,
    val cluster: Cluster,
    val recovery: Disks.Recovery,
    val config: StoreConfig)
