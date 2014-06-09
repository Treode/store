package com.treode.cluster

import com.treode.pickle.Picklers

private case class Hello (host: HostId, cell: CellId)

private object Hello {

  val pickler = {
    import ClusterPicklers._
    tagged [Hello] (
      0x1 -> wrap (tuple (hostId, cellId))
          .build (v => new Hello (v._1, v._2))
          .inspect (v => (v.host, v.cell)))
  }}
