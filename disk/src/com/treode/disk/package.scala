package com.treode

import java.nio.file.{Path, StandardOpenOption}
import java.util.concurrent.ExecutorService

import com.google.common.hash.Hashing
import com.treode.async.AsyncIterator
import com.treode.async.io.File

package disk {

  private class AlreadyAttachedException (paths: Seq [Path]) extends IllegalArgumentException {
    override def getMessage = s"Disks already attached: ${paths mkString ", "}"
  }

  private class CannotDrainAllException extends IllegalArgumentException {
    override def getMessage = "Cannot drain all disks."
  }

  private class CellMismatchException (expected: CellId, found: CellId) extends Exception {
    override def getMessage = s"Expected $expected, found $found."
  }

  private class DiskFullException extends Exception {
    override def getMessage = "Disk full."
  }

  private class ExtraDisksException (paths: Seq [Path]) extends IllegalArgumentException {
    override def getMessage = s"Extra disks in reattachment: ${paths mkString ", "}"
  }

  private class InconsistentSuperBlocksException extends Exception {
    override def getMessage = "Inconsistent superblocks."
  }

  private class MissingDisksException (paths: Seq [Path]) extends IllegalArgumentException {
    override def getMessage = s"Missing disks in reattachment: ${paths mkString ", "}"
  }

  private class NoSuperBlocksException extends Exception {
    override def getMessage = "No superblocks."
  }

  private class NotAttachedException (paths: Seq [Path]) extends IllegalArgumentException {
    override def getMessage = s"No such disks are attached: ${paths mkString ", "}"
  }

  private class OversizedPageException (maximum: Int, found: Int) extends IllegalArgumentException {
    override def getMessage = s"The page of $found bytes exceeds the limit of $maximum bytes."
  }

  private class OversizedRecordException (maximum: Int, found: Int) extends IllegalArgumentException {
    override def getMessage = s"The record of $found bytes exceeds the limit of $maximum bytes."
  }

  private class PageLedgerOverflowException extends Exception {
    override def getMessage = "The page ledger is to large for its allocated disk space."
  }

  private class SuperblockOverflowException extends Exception {
    override def getMessage = "The superblock data is to large for its allocated disk space."
  }}

package object disk {

  private [disk] type LogDispatcher = Dispatcher [PickledRecord]
  private [disk] type PageDispatcher = Dispatcher [PickledPage]
  private [disk] type ReplayIterator = AsyncIterator [(Long, Unit => Any)]

  private [disk] val checksum = Hashing.murmur3_32

  private [disk] implicit class RichIteratable [A] (iter: Iterable [A]) {

    def mapBy [K] (k: A => K): Map [K, A] = {
      val b = Map.newBuilder [K, A]
      iter foreach (x => b += (k (x) -> x))
      b.result
    }

    def mapValuesBy [K, V] (k: A => K) (v: A => V): Map [K, V] = {
      val b = Map.newBuilder [K, V]
      iter foreach (x => b += (k (x) -> v (x)))
      b.result
    }

    def setBy [B] (f: A => B): Set [B] = {
      val b = Set.newBuilder [B]
      iter foreach (x => b += f (x))
      b.result
    }}

  private [disk] def openFile (item: (Path, DiskGeometry), exec: ExecutorService) = {
    val (path, config) = item
    import StandardOpenOption.{CREATE, READ, WRITE}
    val file = File.open (path, exec, CREATE, READ, WRITE)
    (path, file, config)
  }

  private [disk] def reopenFile (path: Path, exec: ExecutorService) = {
    import StandardOpenOption.{READ, WRITE}
    File.open (path, exec, READ, WRITE)
  }}
