package com.treode.disk

import scala.collection.mutable.Builder

import com.treode.async.Callback
import com.treode.async.implicits._

private abstract class AbstractReleaser [A] {

  private class Epoch (var parties: Int, val segments: Seq [A]) {
    override def toString = s"Epoch($parties, $segments)"
  }

  private var epochs = Map.empty [Int, Epoch]
  private var epoch = 0
  private var parties = 0

  private def leave (epoch: Int, builder: Builder [A, _]) {
    if (epoch == this.epoch) {
      parties -= 1
    } else {
      epochs.get (epoch) match {
        case Some (past) =>
          past.parties -= 1
          if (past.parties == 0) {
            epochs -= epoch
            builder ++= past.segments
            leave (epoch+1, builder)
          }
        case None =>
          throw new AssertionError ("Party left forgotten epoch")
      }}}

  def _leave (epoch: Int): Seq [A] = synchronized {
    val builder = Seq.newBuilder [A]
    leave (epoch, builder)
    builder.result
  }

  def _release (segments: Seq [A]): Seq [A] = synchronized {
    if (epochs.isEmpty && parties == 0) {
      segments
    } else {
      epochs += epoch -> new Epoch (parties, segments)
      epoch += 1
      parties = 1
      Seq.empty [A]
    }}

  def _join(): Int = synchronized {
    parties += 1
    epoch
  }}
