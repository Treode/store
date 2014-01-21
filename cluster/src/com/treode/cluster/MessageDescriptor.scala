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
class MessageDescriptor [M] (val id: MailboxId, val pmsg: Pickler [M]) {

  private [cluster] def register (m: MailboxRegistry) (f: (M, Peer) => Any): Unit =
    m.register (pmsg, id) (f)

  def register (f: (M, Peer) => Any) (implicit h: Host): Unit =
    h.register (this) (f)

  def apply (msg: M) = MessageSender (id, pmsg, msg)
}
