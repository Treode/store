package com.treode.cluster

import com.treode.pickle.{Pickler, Picklers}

class RumorDescriptor [M] (val id: MailboxId, val pmsg: Pickler [M]) {

  def listen (f: (M, Peer) => Any) (implicit c: Cluster): Unit =
    c.listen (this) (f)

  def spread (msg: M) (implicit c: Cluster): Unit =
    c.spread (this) (msg)
}

object RumorDescriptor {

  def apply [M] (id: MailboxId, pval: Pickler [M]): RumorDescriptor [M] =
    new RumorDescriptor (id, pval)
}
