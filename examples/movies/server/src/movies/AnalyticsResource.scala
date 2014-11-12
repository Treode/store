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

package movies

import com.treode.async.{Async, Scheduler}, Async.supply
import com.treode.store.Store
import com.twitter.finagle.http.{Method, Request, Response, Status}

import movies.{PhysicalModel => PM}

object AnalyticsResource {

  def apply (router: Router) (implicit scheduler: Scheduler, store: Store) {

    router.register ("/rdd/actors") { request =>
      request.method match {
        case Method.Get =>
          val rt = request.lastModificationBefore
          supply (respond.json (request, PM.Actor.list (rt)))
        case _ =>
          supply (respond (request, Status.MethodNotAllowed))
      }}

    router.register ("/rdd/movies") { request =>
      request.method match {
        case Method.Get =>
          val rt = request.lastModificationBefore
          supply (respond.json (request, PM.Movie.list (rt)))
        case _ =>
          supply (respond (request, Status.MethodNotAllowed))
      }}

    router.register ("/rdd/roles") { request =>
      request.method match {
        case Method.Get =>
          val rt = request.lastModificationBefore
          supply (respond.json (request, PM.Roles.list (rt)))
        case _ =>
          supply (respond (request, Status.MethodNotAllowed))
      }}}}
