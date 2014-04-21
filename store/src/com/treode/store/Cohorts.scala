package com.treode.store

import com.treode.cluster.HostId
import com.treode.pickle.Pickler

import Integer.highestOneBit

class Cohorts private (
    val cohorts: Array [Cohort],
    val version: Int
) {

  private val mask = cohorts.length - 1

  def place (id: Int): Int =
    id & mask

  def locate (id: Int): Cohort =
    cohorts (id & mask)

  def residents (host: HostId): Residents = {
    val nums = for ((c, i) <- cohorts.zipWithIndex; if c.hosts contains host) yield i
    new Residents (nums.toSet, cohorts.size - 1)
  }}

object Cohorts {

  def apply (cohorts: Array [Cohort], version: Int): Cohorts = {

    require (
        highestOneBit (cohorts.length) == cohorts.length,
        "Number of cohorts must be a power of two.")
    require (
        version > 0,
        "Atlas version must be positive.")

    new Cohorts (cohorts, version)
  }

  val empty = new Cohorts (Array (Cohort.empty), 0)

  val pickler = {
    import StorePicklers._
    wrap (array (cohort), uint)
    .build (v => new Cohorts (v._1, v._2))
    .inspect (v => (v.cohorts, v.version))
  }}
