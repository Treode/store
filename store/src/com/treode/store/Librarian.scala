package com.treode.store

import scala.util.{Failure, Success}

import com.treode.async.{Async, Callback, Fiber, Scheduler}
import com.treode.cluster.{Cluster, HostId, Peer}
import com.treode.store.catalog.Catalogs

import Async.{guard, supply}
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
    val next = atlas.advance (receipts, moves)
    if (next.isEmpty) return
    catalogs.issue (Atlas.catalog) (next.get.version, next.get) run {
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
  }

  def issueAtlas (cohorts: Array [Cohort]): Unit = fiber.execute {
    var issued = false
    var tries = 0
    scheduler.whilst (!issued) {
      val change =
        if (library.atlas.version == 0)
          Some (Atlas (cohorts, 1))
        else
          library.atlas.change (cohorts)
      if (change.isEmpty) {
        supply (issued = true)
      } else {
        val save = (library.atlas, library.residents)
        val atlas = change.get
        if (library.atlas.version == 0) {
          library.atlas = atlas
          library.residents = atlas.residents (localId)
        }
        catalogs
            .issue (Atlas.catalog) (atlas.version, atlas)
            .map (_ => issued = true)
            .recover {
              case _: Throwable if !(library.atlas eq atlas) && library.atlas == atlas =>
                issued = true
              case t: StaleException if tries < 16 =>
                if (library.atlas eq atlas) {
                  library.atlas = save._1
                  library.residents = save._2
                }
                tries += 1
              case t: TimeoutException if tries < 16 =>
                if (library.atlas eq atlas) {
                  library.atlas = save._1
                  library.residents = save._2
                }
                tries += 1
            }}
    } .run (ignore)
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
