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

package com.treode.disk

import java.nio.file.Path
import java.util.logging.{Level, Logger}, Level.{INFO, WARNING}

/** Logging and metrics. Users can connect their own logging and metrics mechanisms. We stub it for
  * testing.
  */
class DiskEvents {

  val logger = Logger.getLogger ("com.treode.disk")

  def changedDisks (attached: Set [Path], detached: Set [Path], draining: Set [Path]) {
    if (!attached.isEmpty)
      logger.log (INFO, s"Attached disks: ${attached map (quote _) mkString ", "}")
    if (!detached.isEmpty)
      logger.log (INFO, s"Detached disks: ${detached map (quote _) mkString ", "}")
    if (!draining.isEmpty)
      logger.log (INFO, s"Draining disks: ${draining map (quote _) mkString ", "}")
  }

  def noCompactorFor (id: TypeId): Unit =
    logger.log (WARNING, s"No compactor for $id.")

  def reattachingDisks (paths: Set [Path]): Unit =
    logger.log (INFO, s"Reattaching disks: ${paths map (quote _) mkString ", "}")
}

private object DiskEvents {

  val default = new DiskEvents
}
