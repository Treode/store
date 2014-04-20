package com.treode.store

import com.treode.async.Async
import com.treode.cluster.Cluster
import com.treode.store.atlas.AtlasKit
import com.treode.pickle.Pickler

private trait Atlas {

  def rebalance (f: Array [Cohort] => Async [Unit])

  def place (id: Int): Int

  def locate (id: Int): Cohort

  def residents: Residents

  def issue (cohorts: Array [Cohort]) (implicit catalogs: Catalogs): Async [Unit]

  def place [A] (p: Pickler [A], v: A): Int =
    place (p.murmur32 (v))

  def locate [A] (p: Pickler [A], v: A): Cohort =
    locate (p.murmur32 (v))
}

private object Atlas {

  def recover () (implicit cluster: Cluster, catalogs: Catalogs.Recovery): Atlas =
   AtlasKit.recover()
}
