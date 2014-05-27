package com.treode.store

import java.util.{Arrays, Objects}
import com.treode.cluster.{HostId, RumorDescriptor}
import com.treode.pickle.Pickler

import Cohort.{Issuing, Moving, Settled}
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

  def quorum (hosts: Set [HostId]): Boolean =
    cohorts forall (_.quorum (hosts))

  def settled: Boolean =
    cohorts forall (_.settled)

  def issuing: Boolean =
    cohorts exists (_.issuing)

  def moving: Boolean =
    cohorts exists (_.moving)

  def residents (host: HostId): Residents = {
    val nums = for ((c, i) <- cohorts.zipWithIndex; if c.hosts contains host) yield i
    new Residents (nums.toSet, cohorts.size - 1)
  }

  private def cohortsAsObjects: Array [Object] =
    cohorts.asInstanceOf [Array [Object]]

  override def equals (other: Any): Boolean =
    other match {
      case that: Atlas =>
         Arrays.equals (cohortsAsObjects, that.cohortsAsObjects) && version == that.version
      case _ =>
        false
    }

  override def hashCode: Int =
    Objects.hashCode (Arrays.hashCode (cohortsAsObjects), version)

  override def toString: String =
    s"Atlas($version,\n   ${cohorts mkString "\n    "})"
}

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
  }

  val received = {
    import StorePicklers._
    RumorDescriptor (0x6E73ED5CBD7E357CL, uint)
  }

  val moved = {
    import StorePicklers._
    RumorDescriptor (0x24111C0F37C3C0E1L, uint)
  }}
