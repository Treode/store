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

package com.treode.twitter.server.handler

import java.nio.file.Path

import com.treode.disk.DriveAttachment
import com.treode.store.{Cohort, Store, StoreController}
import com.treode.twitter.finagle.http.{RichRequest, mapper}
import com.treode.twitter.util._
import com.twitter.finagle.Service
import com.twitter.finagle.http.{Method, Request, Response, Status}
import com.twitter.util.Future

class AtlasHandler (controller: StoreController) extends Service [Request, Response] {

  def apply (req: Request): Future [Response] = {
    req.method match {

      case Method.Get =>
        Future.value (respond.json (req, controller.cohorts))

      case Method.Put =>
        controller.cohorts = req.readJson [Array [Cohort]]
        Future.value (respond (req, Status.Ok))

      case _ =>
        Future.value (respond (req, Status.MethodNotAllowed))
    }}}

class DrivesHandler (controller: StoreController) extends Service [Request, Response] {

  def apply (req: Request): Future [Response] = {
    req.method match {

      case Method.Get =>
        controller.drives
          .map (drives => respond.json (req, drives))
          .toTwitterFuture

      case _ =>
        Future.value (respond (req, Status.MethodNotAllowed))
    }}}

class DrivesAttachHandler (controller: StoreController) extends Service [Request, Response] {

  def apply (req: Request): Future [Response] = {
    req.method match {

      case Method.Post =>
        val drives = req.readJson [Seq [DriveAttachment]]
        controller.attach (drives: _*)
          .map (_ => respond (req, Status.Ok))
          .toTwitterFuture

      case _ =>
        Future.value (respond (req, Status.MethodNotAllowed))
    }}}

class DrivesDrainHandler (controller: StoreController) extends Service [Request, Response] {

  def apply (req: Request): Future [Response] = {
    req.method match {

      case Method.Post =>
        val paths = req.readJson [Seq [Path]]
        controller.drain (paths: _*)
          .map (_ => respond (req, Status.Ok))
          .toTwitterFuture

      case _ =>
        Future.value (respond (req, Status.MethodNotAllowed))
    }}}

class TablesHandler (controller: StoreController) extends Service [Request, Response] {

  def apply (req: Request): Future [Response] = {
    req.method match {

      case Method.Get =>
        controller.tables
          .map (tables => respond.json (req, tables))
          .toTwitterFuture

      case _ =>
        Future.value (respond (req, Status.MethodNotAllowed))
    }}}