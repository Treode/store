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

import com.treode.async.{Async, Scheduler}, Async.async
import com.treode.disk.exceptions.OversizedPageException

private class PageDispatcher (implicit scheduler: Scheduler, config: DiskConfig)
extends Dispatcher [PickledPage] {

  import config.maximumPageBytes

  def write [P] (desc: PageDescriptor [P], obj: ObjectId, gen: Long, page: P): Async [Position] =
    async { cb =>
      val p = PickledPage (desc, obj, gen, page, cb)
      if (p.byteSize > maximumPageBytes)
        throw new OversizedPageException (maximumPageBytes, p.byteSize)
      send (p)
    }}
