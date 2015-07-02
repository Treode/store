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

package com.treode.server

import com.twitter.finagle.Service
import com.twitter.finagle.http.{Method, Request, Response, Status}
import com.twitter.util.Future

import SchemaParser.{CompilerFailure, CompilerSuccess, parse}

class SchemaHandler (librarian: Librarian) extends Service [Request, Response] {

  def apply (req: Request): Future [Response] = {
    req.method match {

      case Method.Get =>
        Future.value (respond.plain (req, librarian.schema.toString))

      case Method.Put =>
        parse (req.contentString) match {
          case CompilerSuccess (schema) =>
            librarian.schema = schema
            Future.value (respond (req, Status.Ok))
          case CompilerFailure (errors) =>
            Future.value (respond (req, errors))
        }

      case _ =>
        Future.value (respond (req, Status.MethodNotAllowed))
    }}}
