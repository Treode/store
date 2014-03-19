package com.treode.disk

class DisksConfig private (
    val superBlockBits: Int,
    val checkpointBytes: Int,
    val checkpointEntries: Int,
    val cleaningFrequency: Int,
    val cleaningLoad: Int
) {

  val superBlockBytes = 1 << superBlockBits
  val superBlockMask = superBlockBytes - 1
  val diskLeadBytes = 1 << (superBlockBits + 1)

  def checkpoint (bytes: Int, entries: Int): Boolean =
    bytes > checkpointBytes || entries > checkpointEntries

  def clean (segments: Int): Boolean =
    segments >= cleaningFrequency

  override def toString =
    s"DisksConfig($superBlockBits, $checkpointBytes, $checkpointEntries, $cleaningFrequency, $cleaningLoad)"
}

object DisksConfig {

  def apply (
      superBlockBits: Int,
      checkpointBytes: Int,
      checkpointEntries: Int,
      cleaningFrequency: Int,
      cleaningLoad: Int
  ): DisksConfig = {

    require (superBlockBits > 0, "A superblock must have more than 0 bytes")
    require (checkpointBytes > 0, "The checkpoint interval must be more than 0 bytes")
    require (checkpointEntries > 0, "The checkpoint interval must be more than 0 entries")
    require (cleaningFrequency > 0, "The cleaning frequency must be more than 0 segments")
    require (cleaningLoad > 0, "The cleaning load must be more than 0 segemnts")

    new DisksConfig (
        superBlockBits,
        checkpointBytes,
        checkpointEntries,
        cleaningFrequency,
        cleaningLoad)
  }

  val pickler = {
    import DiskPicklers._
    wrap (uint, uint, uint, uint, uint)
    .build ((apply _).tupled)
    .inspect (v => (
        v.superBlockBits, v.checkpointBytes, v.checkpointEntries, v.cleaningFrequency,
        v.cleaningLoad))
  }}
