/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.treode

import java.nio.file.{Path, StandardOpenOption}
import java.util.concurrent.ExecutorService
import java.util.logging.{Level, Logger}

import com.google.common.hash.Hashing
import com.treode.async.Scheduler
import com.treode.async.io.File

import Level.INFO

package disk {

  case class Compaction (obj: ObjectId, gens: Set [Long])

  case class DiskSystemDigest (drives: Seq [DriveDigest])

  case class DriveAttachment (path: Path, geometry: DriveGeometry)

  case class DriveChange (attaches: Seq [DriveAttachment], drains: Seq [Path])

  case class DriveDigest (path: Path, geometry: DriveGeometry, allocated: Int, draining: Boolean)

  class ControllerException (val message: String) extends IllegalArgumentException (message)

  class DiskFullException extends Exception {
    override def getMessage = "Disk full."
  }

  class DisksClosedException extends IllegalStateException {
    override def getMessage = "The disk system is closed."
  }

  class ExtraDisksException (val paths: Seq [Path]) extends IllegalArgumentException {
    override def getMessage = s"Extra disk in reattachment: ${paths mkString ", "}"
  }

  class InconsistentSuperBlocksException extends Exception {
    override def getMessage = "Inconsistent superblocks."
  }

  class MissingDisksException (paths: Seq [Path]) extends IllegalArgumentException {
    override def getMessage = s"Missing disks in reattachment: ${paths mkString ", "}"
  }

  class NoSuperBlocksException extends Exception {
    override def getMessage = "No superblocks."
  }

  class OversizedPageException (maximum: Int, found: Int) extends IllegalArgumentException {
    override def getMessage = s"The page of $found bytes exceeds the limit of $maximum bytes."
  }

  class OversizedRecordException (maximum: Int, found: Int) extends IllegalArgumentException {
    override def getMessage = s"The record of $found bytes exceeds the limit of $maximum bytes."
  }

  class PageLedgerOverflowException extends Exception {
    override def getMessage = "The page ledger is to large for its allocated disk space."
  }

  class SuperblockOverflowException extends Exception {
    override def getMessage = "The superblock data is to large for its allocated disk space."
  }}

package object disk {

  private [disk] type LogDispatcher = Dispatcher [PickledRecord]
  private [disk] type PageDispatcher = Dispatcher [PickledPage]

  private [disk] val checksum = Hashing.murmur3_32

  private [disk] def openFile (path: Path, geom: DriveGeometry) (implicit scheduler: Scheduler) = {
    import StandardOpenOption.{CREATE, READ, WRITE}
    File.open (path, CREATE, READ, WRITE)
  }

  private [disk] def reopenFile (path: Path) (implicit scheduler: Scheduler) = {
    import StandardOpenOption.{READ, WRITE}
    File.open (path, READ, WRITE)
  }

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

  private [disk] object log {

    val logger = Logger.getLogger ("com.treode.disk")

    def attachedDrives (paths: Set [Path]): Unit =
      logger.log (INFO, s"Attached drives ${paths mkString ", "}")

    def detachedDrives (paths: Set [Path]): Unit =
      logger.log (INFO, s"Detached drives ${paths mkString ", "}")

    def drainingDrives (paths: Set [Path]): Unit =
      logger.log (INFO, s"Draining drives ${paths mkString ", "}")

    def initializedDrives (paths: Set [Path]): Unit =
      logger.log (INFO, s"Initialized drives ${paths mkString ", "}")

    def openedDrives (paths: Set [Path]): Unit =
      logger.log (INFO, s"Opened drives ${paths mkString ", "}")
  }}
