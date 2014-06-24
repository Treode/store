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

package com.treode.store

import org.joda.time.DateTime

trait Epoch {

  def limit: TxClock
}

object Epoch {

  object UnixEpoch extends Epoch {

    def limit: TxClock = TxClock.MinValue
  }

  object StartOfPreviousHour extends Epoch {

    def limit: TxClock =
      DateTime.now.minusHours (1)
        .withMillisOfSecond (0)
        .withSecondOfMinute (0)
        .withMinuteOfHour (0)
  }

  object StartOfPreviousMonth extends Epoch {

    def limit: TxClock =
      DateTime.now.minusMonths (1)
        .withTimeAtStartOfDay()
        .withDayOfMonth (0)
  }

  object StartOfPreviousWeek extends Epoch {

    def limit: TxClock =
      DateTime.now.minusWeeks (1)
        .withTimeAtStartOfDay()
        .withDayOfWeek (0)
  }

  object StartOfYesterday extends Epoch {

    def limit: TxClock =
      DateTime.now.minusDays (1)
        .withTimeAtStartOfDay()
  }}
