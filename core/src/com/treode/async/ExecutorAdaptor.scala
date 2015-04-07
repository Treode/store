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

package com.treode.async

import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

private class ExecutorAdaptor (executor: ScheduledExecutorService) extends Scheduler {

  def execute (task: Runnable) =
    executor.execute (task)

  def delay (millis: Long, task: Runnable) =
    executor.schedule (task, millis, TimeUnit.MILLISECONDS)

  def at (millis: Long, task: Runnable) {
    val t = System.currentTimeMillis
    if (millis < t)
      executor.execute (task)
    else
      executor.schedule (task, millis - t, TimeUnit.MILLISECONDS)
  }}
