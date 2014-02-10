package com.treode.cluster

import com.treode.cluster.messenger.MailboxRegistry
import com.treode.pickle.Pickler

/**
 * Registered mailboxes must use an id with the highest eight bits set.  To make a fresh
 * mailbox id when hacking:
 * '''
 * head -c 7 /dev/urandom | hexdump -e '"0xFF" 7/1 "%02X" "L\n"'
 * '''
 */
class MessageDescriptor [M] private (val id: MailboxId, val pmsg: Pickler [M]) {

  private [cluster] def listen (m: MailboxRegistry) (f: (M, Peer) => Any): Unit =
    m.listen (pmsg, id) (f)

  def listen (f: (M, Peer) => Any) (implicit c: Cluster): Unit =
    c.listen (this) (f)

  def apply (msg: M) = MessageSender (id, pmsg, msg)
}

object MessageDescriptor {

  def apply [M] (id: MailboxId, pmsg: Pickler [M]): MessageDescriptor [M] =
    new MessageDescriptor (id, pmsg)
}
