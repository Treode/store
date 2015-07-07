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

import com.treode.twitter.finagle.http.filter.{JsonExceptionFilter, NettyToFinagle}
import com.twitter.finagle.Service
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.http.filter.ExceptionFilter
import com.twitter.server.{AdminHttpServer, TwitterServer}, AdminHttpServer.Route

trait AdminableServer extends TwitterServer {

  private var adminRouteRegistry = Seq.empty [Route]

  override def routes =
    super.routes ++ adminRouteRegistry

  def addAdminHandler (pattern: String, alias: String, service: Service [Request, Response]) {
    val handler =
        NettyToFinagle andThen
        ExceptionFilter andThen
        JsonExceptionFilter andThen
        service
    adminRouteRegistry :+= Route (pattern, handler, alias, Some ("Treode"), true)
  }}
