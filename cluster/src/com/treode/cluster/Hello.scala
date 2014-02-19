package com.treode.cluster

import com.treode.pickle.Picklers

private case class Hello (id: HostId)

private object Hello {

  val pickler = {
    import Picklers._
    tagged [Hello] (
      0x1 -> wrap (HostId.pickler) .build (apply _) .inspect (_.id))
  }}
