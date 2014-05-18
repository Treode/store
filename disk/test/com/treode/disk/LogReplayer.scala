package com.treode.disk

import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._

import LogTracker.{pagers, records}

class LogReplayer {

  private var primary = Map.empty [Int, Int]
  private var secondary = Map.empty [Int, Int]
  private var reread = Option.empty [Position]
  private var round = 0
  private var gen = 0

  def put (n: Int, g: Int, k: Int, v: Int) {
    assert (n >= round)
    round = n
    if (g < gen && reread.isEmpty) {
      secondary += k -> v
    } else if (g == gen) {
      primary += k -> v
    } else if (g > gen) {
      gen = g
      secondary ++= primary
      primary = Map.empty
      primary += k -> v
    }}

  def checkpoint (gen: Int, pos: Position) {
    if (gen == this.gen - 1) {
      this.secondary = Map.empty
      this.reread = Some (pos)
    } else if (gen >= this.gen) {
      this.gen = gen + 1
      this.primary = Map.empty
      this.secondary = Map.empty
      this.reread = Some (pos)
    }}

  def attach (implicit recovery: Disk.Recovery) {
    records.put.replay ((put _).tupled)
    records.checkpoint.replay ((checkpoint _).tupled)
  }

  def check (tracker: LogTracker) (implicit scheduler: StubScheduler, disks: Disk) {
    reread match {
      case Some (pos) =>
        val saved = pagers.table.read (pos) .pass
        tracker.check (saved ++ secondary ++ primary)
      case None =>
        tracker.check (secondary ++ primary)
    }}

  override def toString = s"Replayer(\n  $reread\n  $primary)"
}
