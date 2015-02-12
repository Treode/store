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
import com.treode.store.{StaleException, TxClock}
import com.twitter.finagle.http.{Method, Request, Response, Status}

import movies.{DisplayModel => DM}

object MovieResource {

  def apply (host: HostId, movies: MovieStore, router: Router) {

    def get (request: Request, id: String): Async [Response] = {
      val rt = request.requestTxClock
      val ct = request.conditionTxClock (TxClock.MinValue)
      for {
        (vt, movie) <- movies.readMovie (rt, id)
      } yield {
        movie match {
          case Some (v) if ct < vt =>
            respond.json (request, vt, v)
          case Some (v) =>
            respond (request, Status.NotModified)
          case None =>
            respond (request, Status.NotFound)
        }}}

    def query (request: Request): Async [Response] = {
      val rt = request.requestTxClock
      val q = request.query
      for {
        result <- movies.query (rt, q, true, false)
      } yield {
        respond.json (request, result)
      }}

    def post (request: Request): Async [Response] = {
      val xid = request.transactionId (host)
      val ct = request.conditionTxClock (TxClock.now)
      val movie = request.readJson [DM.Movie]
      (for {
        (id, vt) <- movies.create (xid, ct, movie)
      } yield {
        respond.created (request, vt, s"/movie/$id")
      }) .recover {
        case _: StaleException =>
          respond (request, Status.PreconditionFailed)
      }}

    def put (request: Request, id: String): Async [Response] = {
      val xid = request.transactionId (host)
      val ct = request.conditionTxClock (TxClock.now)
      val movie = request.readJson [DM.Movie]
      (for {
        vt <- movies.update (xid, ct, id, movie)
      } yield {
        respond.ok (request, vt)
      }) .recover {
        case _: StaleException =>
          respond (request, Status.PreconditionFailed)
      }}

    router.register ("/movie/") { request =>
      val id = request.id ("/movie/")
      request.method match {
        case Method.Get =>
          get (request, id)
        case Method.Put =>
          (id matches "[a-zA-Z0-9_-]+") orBadRequest (s"Invalid ID: $id")
          put (request, id)
        case _ =>
          supply (respond (request, Status.MethodNotAllowed))
      }}

    router.register ("/movie") { request =>
      request.method match {
        case Method.Get =>
          query (request)
        case Method.Post =>
          post (request)
        case _ =>
          supply (respond (request, Status.MethodNotAllowed))
      }}}}
