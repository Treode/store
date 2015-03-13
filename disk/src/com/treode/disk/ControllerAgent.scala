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
import com.treode.async.Async

private class ControllerAgent (kit: DiskKit, val disk: Disk) extends DiskController  {

  def drives: Async [Seq [DriveDigest]] =
    kit.drives.digest

  def attach (items: DriveAttachment*): Async [Unit] =
    kit.drives.attach (items)

  def drain (items: Path*): Async [Unit] =
    kit.drives.drain (items)

  def shutdown(): Async [Unit] =
    kit.close()
}
