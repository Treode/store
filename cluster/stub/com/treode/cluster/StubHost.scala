package com.treode.cluster

import com.treode.pickle.Pickler

trait StubHost {

  def localId: HostId
  def deliver [M] (p: Pickler [M], from: HostId, mbx: MailboxId, msg: M)
}
