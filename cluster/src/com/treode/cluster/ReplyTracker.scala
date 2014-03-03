package com.treode.cluster

trait ReplyTracker {

  def += (p: Peer)
  def clear()
  def awaiting: Set [HostId]
  def quorum: Boolean
  def unity: Boolean
}

object ReplyTracker {

  private class Isolated extends ReplyTracker {
    def += (p: Peer) = ()
    def clear() = ()
    def awaiting = Set.empty [HostId]
    def quorum = false
    def unity = false

    override def toString = "ReplyTracker.Isolated"
  }

  /** The isolated tracker speaks to no-one and never gets a quorum. */
  val empty: ReplyTracker = new Isolated

  private class Settled (hosts: Set [HostId], nquorum: Int)
  extends ReplyTracker {

    private var hs = hosts

    def += (p: Peer): Unit = hs -= p.id
    def clear(): Unit = hs = hosts
    def awaiting = hs
    def quorum = hs.size < nquorum
    def unity = hs.size == 0

    override def toString =
      s"ReplyTracker.Settled(${(hosts -- hs) mkString ","})"
  }

  private class Moving (origin: Set [HostId], target: Set [HostId], aquorum: Int, tquorum: Int)
  extends ReplyTracker {

    private var os = origin
    private var ts = target

    def += (p: Peer) {
      os -= p.id
      ts -= p.id
    }

    def clear() {
      os = origin
      ts = target
    }

    def awaiting = os ++ ts

    def quorum = os.size < aquorum && ts.size < tquorum

    def unity = os.size == 0 && ts.size == 0

    override def toString =
      s"ReplyTracker.Moving(${(origin ++ target -- os -- ts) mkString ","})"
  }

  def apply (active: Set [HostId], target: Set [HostId]): ReplyTracker =
    if (active == target)
      new Settled (active, (active.size >> 1) + 1)
    else
      new Moving (active, target, (active.size >> 1) + 1, (target.size >> 1) + 1)

  def settled (hosts: Set [HostId]): ReplyTracker =
    new Settled (hosts.toSet, (hosts.size >> 1) + 1)

  def settled (hosts: HostId*): ReplyTracker =
    settled (hosts.toSet)
}
