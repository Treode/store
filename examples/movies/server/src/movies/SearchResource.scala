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

import com.treode.async.Async, Async.supply
import com.treode.cluster.HostId
import com.treode.store.StaleException
import com.twitter.finagle.http.{Method, Request, Response, Status}

object SearchResource {

  def apply (movies: MovieStore, router: Router) {

    def query (request: Request): Async [Response] = {
      val rt = request.requestTxClock
      val q = request.query
      for {
        result <- movies.query (rt, q, true, true)
      } yield {
        respond.json (request, result)
      }}

    router.register ("/search") { request =>
      request.method match {
        case Method.Get =>
          query (request)
        case _ =>
          supply (respond (request, Status.MethodNotAllowed))
      }}}}
