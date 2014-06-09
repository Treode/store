package com.treode.cluster

import com.treode.pickle.{PickleContext, Pickler, Picklers, UnpickleContext}

private trait ClusterPicklers extends Picklers {

  def cellId = CellId.pickler
  def hostId = HostId.pickler
  def portId = PortId.pickler
  def rumorId = RumorId.pickler

  def void [A] = new Pickler [A] {
    def p (v: A, ctx: PickleContext): Unit = throw new IllegalArgumentException
    def u (ctx: UnpickleContext): A = throw new IllegalArgumentException
  }}

private object ClusterPicklers extends ClusterPicklers
