package com.treode.disk

import java.nio.file.Paths
import com.treode.pickle.Picklers

private trait DiskPicklers extends Picklers {

  def path = wrap (string) build (Paths.get (_)) inspect (_.toString)

  def boot = BootBlock.pickler
  def config = DiskDriveConfig.pickler
  def intSet = IntSet.pickler
  def pageGroup = PageGroup.pickler
  def pageLedger = PageLedger.Zipped.pickler
  def pos = Position.pickler
  def typeId = TypeId.pickler
}

private object DiskPicklers extends DiskPicklers
