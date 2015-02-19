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

import java.util.concurrent.Callable
import scala.util.Success

import com.google.common.cache.{CacheBuilder, CacheLoader, Cache}
import com.treode.async.{Async, Future}

import Async.guard

private class PageCache (kit: DiskKit) {
  import kit.config.pageCacheEntries
  import kit.drives

  class Load (desc: PageDescriptor [Any], pos: Position)
  extends Callable [Future [Any]] {
    def call(): Future [Any] =
      drives.fetch (desc, pos) .toFuture
  }

  private val pages = CacheBuilder.newBuilder
      .maximumSize (pageCacheEntries)
      .build()
      .asInstanceOf [Cache [(Int, Long), Future [Any]]]

  def read [P] (desc: PageDescriptor [P], pos: Position): Async [P] =
    guard {
      pages
          .get ((pos.disk, pos.offset), new Load (desc.asInstanceOf [PageDescriptor [Any]], pos))
          .map (v => desc.tpag.runtimeClass.cast (v) .asInstanceOf [P])
    }

  def write [P] (pos: Position, page: P): Unit = {
    val future = new Future [Any]
    future (Success (page))
    pages.put ((pos.disk, pos.offset), future)
  }}
