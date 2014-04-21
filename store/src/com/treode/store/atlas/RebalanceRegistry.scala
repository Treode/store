package com.treode.store.atlas

import java.util.ArrayList

import com.treode.async.{Async, AsyncImplicits}
import com.treode.store.Cohorts

import Async.guard
import AsyncImplicits._

private class RebalanceRegistry {

  private val rebalancers = new ArrayList [Cohorts => Async [Unit]]

  def rebalance (f: Cohorts => Async [Unit]): Unit =
    rebalancers.add (f)

  def rebalance (cohorts: Cohorts): Async [Unit] =
    guard {
      rebalancers.latch.unit foreach (_ (cohorts))
    }}
