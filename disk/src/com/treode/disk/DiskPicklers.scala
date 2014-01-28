package com.treode.disk

import java.nio.file.Paths
import com.treode.pickle.Picklers

private trait DiskPicklers extends Picklers {

  def path = wrap (string) build (Paths.get (_)) inspect (_.toString)

  def allocMeta = Allocator.Meta.pickler
  def boot = BootBlock.pickler
  def config = DiskDriveConfig.pickler
  def logMeta = LogWriter.Meta.pickler
  def pageMeta = PageWriter.Meta.pickler
  def pos = Position.pickler
  def rootMeta = RootRegistry.Meta.pickler
  def typeId = TypeId.pickler
}

private object DiskPicklers extends DiskPicklers
