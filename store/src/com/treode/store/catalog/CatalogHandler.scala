package com.treode.store.catalog

import java.util.ArrayDeque
import scala.collection.JavaConversions._

import com.nothome.delta.{Delta, GDiffPatcher}
import com.treode.async.Scheduler
import com.treode.cluster.MailboxId
import com.treode.store.{Bytes, StoreConfig}
import com.treode.pickle.Pickler

private abstract class CatalogHandler {

  var version = 0
  var bytes = Bytes.empty
  var history = new ArrayDeque [Bytes]

  def dispatch (bytes: Bytes): Unit

  def diff (other: Int): Update = {
    val start = other - version + history.size
    if (start >= history.size) {
      Right (other, Seq.empty)
    } else if (start < 0) {
      Left (version, bytes, history.toSeq)
    } else {
      Right (other, history.drop (start) .toSeq)
    }}

  def patch (version: Int, bytes: Bytes, history: Seq [Bytes]) {
    if (this.version < version) {
      this.version = version
      this.bytes = bytes
      this.history.clear()
      this.history.addAll (history)
      dispatch (bytes)
    }}

  def patch (start: Int, patches: Seq [Bytes]) {
    if (start + patches.length > version) {
      val future = patches drop (version - start)
      for (patch <- future) {
        version += 1
        bytes = CatalogHandler.patch (bytes, patch)
        history.add (patch)
        if (history.size > catalogHistoryLimit)
          history.remove()
      }
      dispatch (bytes)
    }}

  def patch (update: Update): Unit =
    update match {
      case Left ((version, bytes, history)) =>
        patch (version, bytes, history)
      case Right ((start, patches)) =>
        patch (start, patches)
    }

  def issue (version: Int, bytes: Bytes) {
    require (
        version == this.version + 1,
        "Required version ${this.version + 1}, found $version.")
    val patch = CatalogHandler.diff (this.bytes, bytes)
    this.version = version
    this.bytes = bytes
    history.add (patch)
    if (history.size > catalogHistoryLimit)
      history.remove()
  }}

private object CatalogHandler {

  def diff (val0: Bytes, val1: Bytes): Bytes = {
    val differ = new Delta
    differ.setChunkSize (catalogChunkSize)
    Bytes (differ.compute (val0.bytes, val1.bytes))
  }

  def patch (val0: Bytes, diff: Bytes): Bytes = {
    val patcher = new GDiffPatcher
    Bytes (patcher.patch (val0.bytes, diff.bytes))
  }

  def apply [C] (
      id: MailboxId,
      pcat: Pickler [C],
      handler: C => Any
  ) (implicit
      scheduler: Scheduler
  ): CatalogHandler = {
    new CatalogHandler {

      def dispatch (bytes: Bytes): Unit =
        scheduler.execute (handler (bytes.unpickle (pcat)))

      override def toString = s"CatalogHandler($id,$pcat)"
    }}

  def unknown (id: MailboxId): CatalogHandler =
    new CatalogHandler {

      def dispatch (bytes: Bytes): Unit = ()

      override def toString = s"CatalogHandler($id,unknown)"
    }}
