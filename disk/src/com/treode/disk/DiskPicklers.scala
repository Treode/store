package com.treode.disk

import java.nio.file.Paths
import com.treode.pickle.Picklers

private trait DiskPicklers extends Picklers {

  def path = wrap (string) build (Paths.get (_)) inspect (_.toString)

  def boot = BootBlock.pickler
  def intSet = IntSet.pickler
  def geometry = DriveGeometry.pickler
  def objectId = ObjectId.pickler
  def pageGroup = PageGroup.pickler
  def pageLedger = PageLedger.Zipped.pickler
  def pos = Position.pickler
  def typeId = TypeId.pickler
}

private object DiskPicklers extends DiskPicklers
