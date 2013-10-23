package com.treode.cluster

import scala.util.Random

import com.treode.cluster.messenger.{PeerRegistry, MailboxRegistry}
import com.treode.concurrent.Scheduler

trait Host {

  val localId: HostId
  val random: Random
  val scheduler: Scheduler
  val mailboxes: MailboxRegistry
  val peers: PeerRegistry
}
