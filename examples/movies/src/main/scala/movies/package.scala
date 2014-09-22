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

import scala.util.Random

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.smile.SmileFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.treode.store.Bytes
import com.treode.store.util.Froster

package movies {

  class BadRequestException (val message: String) extends Exception {

    override def getMessage(): String = message
  }}

package object movies {

  private val binaryJson = new ObjectMapper (new SmileFactory)
  binaryJson.registerModule (new DefaultScalaModule)

  private val textJson = new ObjectMapper
  textJson.registerModule (DefaultScalaModule)

  implicit class RichAny [A] (value: A) {

    def orDefault (default: A): A =
      if (value == null) default else value

    def toJson: String =
      textJson.writeValueAsString (value)
  }

  implicit class RichFrosterCompanion (obj: Froster.type) {

    def bson [A] (implicit m: Manifest [A]): Froster [A] =
      new Froster [A] {
        private val c = m.runtimeClass.asInstanceOf [Class [A]]
        def freeze (v: A): Bytes = Bytes (binaryJson.writeValueAsBytes (v))
        def thaw (v: Bytes): A = binaryJson.readValue (v.bytes, c)
      }}

  implicit class RichOption [A] (value: Option [A]) {

    def getOrBadRequest (message: String): A =
      value match {
        case Some (v) => v
        case None => throw new BadRequestException (message)
      }}

  implicit class RichRandom (random: Random) {

    def nextPositiveLong(): Long =
      random.nextLong & Long.MaxValue
  }

  implicit class RichSeq [A] (vs: Seq [A]) {

    def merge (p: A => Boolean, v1: A): Option [Seq [A]] =
      vs.find (p) match {
        case Some (v0) if v0 != v1 => Some (v1 +: vs.filterNot (p))
        case Some (_) => None
        case None => Some (v1 +: vs)
      }

    def remove (p: A => Boolean): Option [Seq [A]] =
      vs.find (p) match {
        case Some (v0) => Some (vs.filterNot (p))
        case None => None
      }}

  implicit class RichString (s: String) {

    def fromJson [A] (implicit m: Manifest [A]): A =
      textJson.readValue (s, m.runtimeClass.asInstanceOf [Class [A]])
  }}
