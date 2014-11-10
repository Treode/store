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

package com.treode.twitter.server

import com.treode.twitter.app.StoreKit
import com.treode.twitter.finagle.http.filter._
import com.treode.twitter.server.handler._
import com.twitter.app.App
import com.twitter.finagle.Service
import com.twitter.finagle.http.{HttpMuxer, Request, Response}
import com.twitter.finagle.http.filter.ExceptionFilter

trait TreodeAdmin {
  this: App with StoreKit =>

  private def add (pattern: String, service: Service [Request, Response]): Unit =
    HttpMuxer.addHandler (
      pattern,
      NettyToFinagle andThen
      ExceptionFilter andThen
      JsonExceptionFilter andThen
      service)

  premain {
    add ("/admin/treode/atlas",         new AtlasHandler (controller))
    add ("/admin/treode/drives",        new DrivesHandler (controller))
    add ("/admin/treode/drives/attach", new DrivesAttachHandler (controller))
    add ("/admin/treode/drives/drain",  new DrivesDrainHandler (controller))
    add ("/admin/treode/tables",        new TablesHandler (controller))
  }}
