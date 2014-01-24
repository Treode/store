package com.treode.disk

import java.nio.file.Paths
import com.treode.pickle.Picklers

private trait DiskPicklers extends Picklers {

  def path = wrap (string) build (Paths.get (_)) inspect (_.toString)

  def alloc = Allocator.Meta.pickle
  def boot = BootBlock.pickle
  def config = DiskDriveConfig.pickle
  def log = LogWriter.Meta.pickle
  def pages = PageWriter.Meta.pickle
  def pos = Position.pickle
  def roots = RootRegistry.Meta.pickle
  def typeId = TypeId.pickle
}

private object DiskPicklers extends DiskPicklers
