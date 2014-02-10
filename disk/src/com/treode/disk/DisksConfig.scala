package com.treode.disk

class DisksConfig private (val superBlockBits: Int) {

  val superBlockBytes = 1 << superBlockBits
  val superBlockMask = superBlockBytes - 1
  val diskLeadBytes = 1 << (superBlockBits + 1)

  override def hashCode: Int =
    superBlockBits.hashCode

  override def equals (other: Any): Boolean =
    other match {
      case that: DisksConfig =>
        superBlockBits == that.superBlockBits
      case _ =>
        false
    }

  override def toString = s"DisksConfig($superBlockBits)"
}

object DisksConfig {

  def apply (superBlockBits: Int): DisksConfig = {
    require (superBlockBits > 0, "A superblock must have more than 0 bytes")
    new DisksConfig (superBlockBits)
  }

  val pickler = {
    import DiskPicklers._
    wrap (uint)
    .build (apply _)
    .inspect (v => (v.superBlockBits))
  }}
