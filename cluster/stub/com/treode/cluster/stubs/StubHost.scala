package com.treode.cluster.stubs

import com.treode.cluster.{HostId, PortId}
import com.treode.pickle.Pickler

trait StubHost {

  def localId: HostId
  def deliver [M] (p: Pickler [M], from: HostId, port: PortId, msg: M)
}
