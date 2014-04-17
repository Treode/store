package com.treode.store.atlas

import java.util.ArrayList

import com.treode.async.{Async, AsyncImplicits}
import com.treode.store.Cohort

import Async.guard
import AsyncImplicits._

private class RebalanceRegistry {

  private val rebalancers = new ArrayList [Array [Cohort] => Async [Unit]]

  def rebalance (f: Array [Cohort] => Async [Unit]): Unit =
    rebalancers.add (f)

  def rebalance (cohorts: Array [Cohort]): Async [Unit] =
    guard {
      rebalancers.latch.unit foreach (_ (cohorts))
    }}
