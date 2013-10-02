package com.treode.cluster

import com.treode.cluster.messenger.{PeerRegistry, MailboxRegistry}

trait Host {

  val localId: HostId
  val mailboxes: MailboxRegistry
  val peers: PeerRegistry
}
