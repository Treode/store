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

package com.treode.cluster

import com.treode.async.Backoff
import com.treode.async.misc.RichInt

case class ClusterConfig (
    connectingBackoff: Backoff,
    statsUpdatePeriod: Int
) {

  require (
      statsUpdatePeriod > 0,
      "The stats update period must be more than 0 milliseconds.")
}

object ClusterConfig {

  val suggested = ClusterConfig (
      connectingBackoff = Backoff (3.seconds, 2.seconds, 3.minutes),
      statsUpdatePeriod = 1.minutes)
}
