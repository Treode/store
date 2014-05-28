package com.treode.store

import scala.util.{Failure, Success}

import com.treode.async.{Async, Callback, Fiber, Scheduler}
import com.treode.cluster.{Cluster, HostId, Peer}
import com.treode.store.catalog.Catalogs

import Async.guard
import Callback.ignore
import Cohort._

private class Librarian private (
    rebalance: Atlas => Async [Unit]
) (implicit
    scheduler: Scheduler,
    cluster: Cluster,
    catalogs: Catalogs,
    library: Library
) {

  import cluster.localId
  import library.{atlas, releaser}

  val fiber = new Fiber
  var active = false
  var issuing = false
  var moving = false
  var issued = 0
  var receipts = Map.empty [HostId, Int] .withDefaultValue (0)
  var moves = Map.empty [HostId, Int] .withDefaultValue (0)

  catalogs.listen (Atlas.catalog) (install _)
  Atlas.received.listen (received _)
  Atlas.moved.listen (moved _)

  private def _rebalance (atlas: Atlas): Unit =
    guard [Unit] {
      for {
        _ <- releaser.release()
        _ = Atlas.received.spread (atlas.version)
        _ <- rebalance (atlas)
      } yield fiber.execute {
        if (library.atlas.version == atlas.version)
          Atlas.moved.spread (atlas.version)
      }
    } run (ignore)

  private def advance() {
    if (!active) return
    if (atlas.version != issued) return
    val current = receipts .filter (_._2 == issued) .keySet
    if (!atlas.quorum (current)) return
    val moved = moves .filter (_._2 == issued) .keySet
    var changed = false
    val cohorts =
      for (cohort <- atlas.cohorts) yield
        cohort match {
          case Issuing (origin, targets) =>
            changed = true
            Moving (origin, targets)
          case Moving (origin, targets) if cohort.quorum (moved) =>
            changed = true
            Settled (targets)
          case _ =>
            cohort
      }
    if (!changed) return
    val version = atlas.version + 1
    catalogs.issue (Atlas.catalog) (version, Atlas (cohorts, version)) run {
      case Success (_) => ()
      case Failure (_: StaleException) => install (library.atlas)
      case Failure (_: TimeoutException) => install (library.atlas)
      case Failure (t) => throw t
    }}

  private def install (atlas: Atlas): Unit = fiber.execute {
    if (library.atlas.version <= atlas.version) {
      library.atlas = atlas
      library.residents = atlas.residents (localId)
      if (issued < atlas.version) issued = atlas.version
      _rebalance (atlas)
      active = atlas.cohorts (0) contains localId
      issuing = atlas.issuing
      moving = atlas.moving
      if (issuing || moving) advance()
    }}

  private def received (issue: Int, peer: Peer): Unit = fiber.execute {
    if (issued < issue) issued = issue
    receipts += peer.id -> issue
    if (issuing || moving) advance()
  }

  private def moved (issue: Int, peer: Peer): Unit = fiber.execute {
    if (issued < issue) issued = issue
    moves += peer.id -> issue
    if (moving) advance()
  }}

private object Librarian {

  def apply (
    rebalance: Atlas => Async [Unit]
  ) (implicit
      scheduler: Scheduler,
      cluster: Cluster,
      catalogs: Catalogs,
      library: Library
  ): Librarian =
    new Librarian (rebalance)
}
