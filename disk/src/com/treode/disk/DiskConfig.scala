package com.treode.disk

case class DiskConfig (
    superBlockBits: Int,
    maximumRecordBytes: Int,
    maximumPageBytes: Int,
    checkpointBytes: Int,
    checkpointEntries: Int,
    cleaningFrequency: Int,
    cleaningLoad: Int
) {

  require (
      superBlockBits > 0,
      "A superblock must have more than 0 bytes.")
  require (
      maximumRecordBytes > 0,
      "The maximum record size must be more than 0 bytes.")
  require (
      maximumPageBytes > 0,
      "The maximum page size must be more than 0 bytes.")
  require (
      checkpointBytes > 0,
      "The checkpoint interval must be more than 0 bytes.")
  require (
      checkpointEntries > 0,
      "The checkpoint interval must be more than 0 entries.")
  require (
      cleaningFrequency > 0,
      "The cleaning interval must be more than 0 segments.")
  require (
      cleaningLoad > 0,
      "The cleaning load must be more than 0 segemnts.")

  val superBlockBytes = 1 << superBlockBits
  val superBlockMask = superBlockBytes - 1
  val diskLeadBytes = 1 << (superBlockBits + 1)

  val minimumSegmentBits = {
    val bytes = math.max (maximumRecordBytes, maximumPageBytes)
    Integer.SIZE - Integer.numberOfLeadingZeros (bytes - 1) + 1
  }

  def checkpoint (bytes: Int, entries: Int): Boolean =
    bytes > checkpointBytes || entries > checkpointEntries

  def clean (segments: Int): Boolean =
    segments >= cleaningFrequency
}

object DiskConfig {

  val suggested = DiskConfig (
      superBlockBits = 14,
      maximumRecordBytes = 1<<24,
      maximumPageBytes = 1<<24,
      checkpointBytes = 1<<24,
      checkpointEntries = 10000,
      cleaningFrequency = 7,
      cleaningLoad = 1)
}
