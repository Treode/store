package com.treode.cluster

import com.treode.cluster.concurrent.Scheduler
import com.treode.cluster.messenger.{PeerRegistry, MailboxRegistry}

trait Host {

  val localId: HostId
  val scheduler: Scheduler
  val mailboxes: MailboxRegistry
  val peers: PeerRegistry
}
