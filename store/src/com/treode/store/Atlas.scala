package com.treode.store

import com.treode.async.Async
import com.treode.cluster.Cluster
import com.treode.store.atlas.AtlasKit

private trait Atlas {

  def rebalance (f: Array [Cohort] => Async [Unit])

  def locate (id: Int): Cohort

  def issue (cohorts: Array [Cohort]) (implicit catalogs: Catalogs): Async [Unit]
}

private object Atlas {

  def recover () (implicit cluster: Cluster, catalogs: Catalogs.Recovery): Atlas =
   AtlasKit.recover()
}
