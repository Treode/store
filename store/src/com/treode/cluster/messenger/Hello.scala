package com.treode.cluster.messenger

import com.treode.cluster.HostId
import com.treode.pickle.Picklers

private case class Hello (id: HostId)

private object Hello {

  val pickle = {
    import Picklers._
    tagged [Hello] (
      0x1 -> wrap [HostId, Hello] (HostId.pickle, Hello (_), _.id))
  }}
