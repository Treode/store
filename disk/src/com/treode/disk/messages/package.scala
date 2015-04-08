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

import com.treode.notify.Message

/*
 * A collection of errors that are specific to the Disk package.
 */

package messages {
  case class AlreadyAttached (path: Path) extends Message {
    val en = s"Already attached: ${quote (path)}";
  }

  case class AlreadyAttaching (path: Path) extends Message {
    val en = s"Already attaching: ${quote (path)}";
  }

  case class NotAttached (drive: Path) extends Message {
    val en = s"Not attached: ${quote (drive)}";
  }

  case class AlreadyDraining (drive: Path) extends Message {
    val en = s"Already draining: ${quote (drive)}";
  }
}
