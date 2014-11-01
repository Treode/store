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

import java.util.concurrent.Executors
import org.scalatest.{BeforeAndAfterAll, Suite}

/** Creates a StubScheduler that wraps a single threaded executor before tests runs, and shuts
  * down the executor after tests runs.  Compare to [[StubScheduler.singlethreaded]] which
  * creates an executor per test. Executors are heavy weight; creating too many during testing
  * can consume resources and cause grief.
  */
trait ProvidedSingleThreadScheduler extends BeforeAndAfterAll {
  this: Suite =>

  private val executor = Executors.newSingleThreadScheduledExecutor
  implicit val scheduler = StubScheduler.wrapped (executor)

  override def afterAll() {
    executor.shutdown()
    super.afterAll()
  }}
