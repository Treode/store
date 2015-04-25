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

package com.treode.async.stubs

import java.util.concurrent.{Executors, ScheduledExecutorService}
import scala.concurrent.ExecutionContext

object StubGlobals {

  /** A default ScheduledExecutorService. */
  val executor: ScheduledExecutorService =
    Executors.newScheduledThreadPool (Runtime.getRuntime.availableProcessors)

  /** A default StubScheduler that uses the default executor. */
  implicit val scheduler: StubScheduler =
    StubScheduler.wrapped (executor)

  /** A default ExecutionContext that uses the default executor. */
  implicit val executionContext: ExecutionContext =
    ExecutionContext.fromExecutorService (executor)
}
