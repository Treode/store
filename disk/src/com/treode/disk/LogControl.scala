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

private object LogControl {

  // Tags used to mark the log seqeunce. Random longs to reduce probability of interpreting user
  // records as tags.
  val SegmentTag = 0x6BAC085AD2778965L
  val EntriesTag = 0x2D213306434FBDABL
  val AllocTag = 0x30564E4351A7625BL
  val EndTag = 0x115AF3019CB2F55EL

  // The marker at between batches of records needs space for
  // - the tag (long, 8 bytes).
  val LogTagSize = 8

  // The marker at the start of a log segment needs space for
  // - the tag (long, 8 bytes).
  val SegmentTagSpace = 8

  // The marker between batches of entries needs space for
  // - LogControl.EntriesTag,
  // - the batch number (long, 8 bytes),
  // - a count of bytes (int, 4 bytes), and
  // - a count of records (int, 4 bytes).
  val LogEntriesSpace = LogTagSize + 16

  // The marker for the next segment needs space for
  // - LogControl.AllocTag, and
  // - a segment number (int, 4 bytes).
  val LogAllocSpace = LogTagSize + 4

  // The marker for the end of the log needs space for
  // - LogControl.EndTag.
  val LogEndSpace = LogTagSize
}
