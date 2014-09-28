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

import scala.language.implicitConversions

/** The `alt` package contains classes that make reading and writing the database more convenient.
  * The primary API for a [[Store]] is to read and write keys and values of [[Bytes]], and to
  * manipulate timestamps directly.  The `alt` package includes [[TableDescriptor]] to read and
  * write typed keys and values, and it includes [[Transaction]] to manage timestamps.
  */
package object alt {

  implicit def pairToReadOp [K, V] (pair: (TableDescriptor [K, V], K)): ReadOp = {
    val (d, k) = pair
    ReadOp (d.id, d.key.freeze (k))
  }}