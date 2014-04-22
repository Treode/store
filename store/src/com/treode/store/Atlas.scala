package com.treode.store

import com.treode.cluster.HostId
import com.treode.pickle.Pickler

import Integer.highestOneBit

class Atlas private (
    val cohorts: Array [Cohort],
    val version: Int
) {

  private val mask = cohorts.length - 1

  def place (id: Int): Int =
    id & mask

  def place [A] (p: Pickler [A], v: A): Int =
    place (p.murmur32 (v))

  def locate (id: Int): Cohort =
    cohorts (id & mask)

  def locate [A] (p: Pickler [A], v: A): Cohort =
    locate (p.murmur32 (v))

  def residents (host: HostId): Residents = {
    val nums = for ((c, i) <- cohorts.zipWithIndex; if c.hosts contains host) yield i
    new Residents (nums.toSet, cohorts.size - 1)
  }}

object Atlas {

  def apply (cohorts: Array [Cohort], version: Int): Atlas = {

    require (
        highestOneBit (cohorts.length) == cohorts.length,
        "Number of cohorts must be a power of two.")
    require (
        version > 0,
        "Atlas version must be positive.")

    new Atlas (cohorts, version)
  }

  val empty = new Atlas (Array (Cohort.empty), 0)

  val pickler = {
    import StorePicklers._
    wrap (array (cohort), uint)
    .build (v => new Atlas (v._1, v._2))
    .inspect (v => (v.cohorts, v.version))
  }

  val catalog = {
    import StorePicklers._
    CatalogDescriptor (0x693799787FDC9106L, atlas)
  }}
