package com.treode.cluster

trait Acknowledgements {

  def += (p: Peer)
  def clear()
  def awaiting: Set [HostId]
  def quorum: Boolean
}

object Acknowledgements {

  /** The empty acknowledegments speaks to no-one and never gets a quorum. */
  val empty = new Acknowledgements {

    def += (p: Peer) = ()
    def clear() = ()
    def awaiting = Set.empty [HostId]
    def quorum = false

    override def toString = "Acknowledgements.empty"
  }

  private class Settled (hosts: Set [HostId], nquorum: Int)
      extends Acknowledgements {

    private var hs = hosts

    def += (p: Peer): Unit = hs -= p.id
    def clear(): Unit = hs = hosts
    def awaiting = hs
    def quorum = hs.size < nquorum

    override def toString =
      s"Acknowledgements.Settled(${(hosts -- hs) mkString ","})"
  }

  private class Moving (active: Set [HostId], target: Set [HostId], aquorum: Int, tquorum: Int)
      extends Acknowledgements {

    private var as = active
    private var ts = target

    def += (p: Peer) {
      as -= p.id
      ts -= p.id
    }

    def clear() {
      as = active
      ts = target
    }

    def awaiting = as ++ ts

    def quorum = as.size < aquorum && ts.size < tquorum

    override def toString =
      s"Acknowledgements.Moving(${(active ++ target -- as -- ts) mkString ","})"
  }

  def apply (active: Set [HostId], target: Set [HostId]): Acknowledgements =
    if (active == target)
      new Settled (active, (active.size >> 1) + 1)
    else
      new Moving (active, target, (active.size >> 1) + 1, (target.size >> 1) + 1)

  def settled (hosts: HostId*): Acknowledgements =
    new Settled (hosts.toSet, (hosts.length >> 1) + 1)
}
