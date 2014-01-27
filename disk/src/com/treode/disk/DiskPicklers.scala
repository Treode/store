package com.treode.disk

import java.nio.file.Paths
import com.treode.pickle.Picklers

private trait DiskPicklers extends Picklers {

  def path = wrap (string) build (Paths.get (_)) inspect (_.toString)

  def alloc = Allocator.Meta.pickler
  def boot = BootBlock.pickler
  def config = DiskDriveConfig.pickler
  def log = LogWriter.Meta.pickler
  def pages = PageWriter.Meta.pickler
  def pos = Position.pickler
  def roots = RootRegistry.Meta.pickler
  def typeId = TypeId.pickler
}

private object DiskPicklers extends DiskPicklers
