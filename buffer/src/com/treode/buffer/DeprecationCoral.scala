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

package com.treode.buffer

/**
  * See https://issues.scala-lang.org/browse/SI-7934
  */
@deprecated ("", "")
private object DeprecationCoral {

  def getBytes (src: String, srcBegin: Int, srcEnd: Int, dst: Array [Byte], dstBegin: Int) =
    src.getBytes (srcBegin, srcEnd, dst, dstBegin)
}
