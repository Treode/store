package com.treode.disk

import java.nio.file.Path
import java.util.ArrayList
import java.util.concurrent.ExecutorService

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.async.io.File

import Async.async
import Disks.Launch

private class RecoveryBuilder (implicit scheduler: Scheduler, config: DisksConfig)
extends Disks.Recovery {

  private val records = new RecordRegistry
  private val loaders = new ReloadRegistry
  private var open = true

  def requireOpen(): Unit =
    require (open, "Recovery has already begun.")

  def reload [B] (desc: RootDescriptor [B]) (f: B => Any) {
    requireOpen()
    loaders.reload (desc) (f)
  }

  def replay [R] (desc: RecordDescriptor [R]) (f: R => Any): Unit = {
    requireOpen()
    records.replay (desc) (f)
  }

  def close (cb: Callback [Launch]): RecoveryAgent = {
    open = false
    new RecoveryAgent (records, loaders, cb)
  }

  def reattach (items: Seq [(Path, File)]): Async [Launch] =
    async (close (_) .reattach (items))

  def reattach (items: Seq [Path], exec: ExecutorService): Async [Launch] =
    async (close (_) .reattach (items, exec))

  def attach (items: Seq [(Path, File, DiskGeometry)]): Async [Launch] =
    async (close (_) .attach (items))

  def attach (items: Seq [(Path, DiskGeometry)], exec: ExecutorService): Async [Launch] =
    async (close (_) .attach (items, exec))
}
