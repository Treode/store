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

package com.treode.jackson

import com.treode.store.Cohort
import org.scalatest.FreeSpec

import Cohort._

class CohortSpec extends FreeSpec with ModuleSpec {

  "Deserializing a cohort should" - {

    "reject an empty object" in {
      reject [Cohort] ("{}")
    }

    "reject an integer" in {
      reject [Cohort] ("1")
    }

    "reject a float" in {
      reject [Cohort] ("1.0")
    }

    "reject a string" in {
      reject [Cohort] ("\"hello\"")
    }

    "reject an array" in {
      reject [Cohort] ("[]")
    }}

  "Serializing an empty cohort should" - {

    "produce an object with state:empty" in {
      assertString ("""{"state":"empty"}""") (Empty)
    }}

  "Deserializing an empty cohort should" - {

    "work" in {
      accept [Cohort] (Empty) {
        """{"state":"empty"}"""
      }}

    "reject an object with hosts" in {
      reject [Cohort] {
        """{"state":"empty", "hosts":[1]}"""
      }}

    "reject an object with an origin" in {
      reject [Cohort] {
        """{"state":"empty", "origin":[1]}"""
      }}

    "reject an object with a target" in {
      reject [Cohort] {
        """{"state":"empty", "target":[1]}"""
      }}}

  "Serializing a settled cohort should" - {

    "produce an object with state:settled and hosts" in {
      assertString ("""{"state":"settled","hosts":["0x0000000000000001"]}""") {
        settled (1)
      }}}

  "Deserializing a settled cohort should" - {

    "work" in {
      accept (settled (1)) {
        """{"state":"settled", "hosts":[1]}"""
      }}

    "reject an object with no hosts" in {
      reject [Cohort] {
        """{"state":"settled", "hosts":[]}"""
      }}

    "reject an object with an origin" in {
      reject [Cohort] {
        """{"state":"settled", "origin":[1]}"""
      }}

    "reject an object with a target" in {
      reject [Cohort] {
        """{"state":"settled", "target":[1]}"""
      }}}

  "Serializing an issuing cohort should" - {

    "produce an object with state:issuing, origin and target" in {
      assertString ("""{"state":"issuing","origin":["0x0000000000000001"],"target":["0x0000000000000002"]}""") {
        issuing (1) (2)
      }}}

  "Deserializing an issuing cohort should" - {

    "work" in {
      accept (issuing (1) (2)) {
        """{"state":"issuing", "origin":[1], "target":[2]}"""
      }}

    "reject an object with hosts" in {
      reject [Cohort] {
        """{"state":"issuing", "hosts":[1], "origin":[1], "target":[2]"}"""
      }}

    "reject an object with no origin" in {
      reject [Cohort] {
        """{"state":"issuing", "origin":[], "target":[2]"}"""
      }}

    "reject an object with no target" in {
      reject [Cohort] {
        """{"state":"issuing", "origin":[1], "target":[]"}"""
      }}}

  "Serializing a moving cohort should" - {

    "produce an object with state:moving, origin and target" in {
      assertString ("""{"state":"moving","origin":["0x0000000000000001"],"target":["0x0000000000000002"]}""") {
        moving (1) (2)
      }}}

  "Deserializing a moving cohort should" - {

    "work" in {
      accept (moving (1) (2)) {
        """{"state":"moving", "origin":[1], "target":[2]}"""
      }}

    "reject an object with hosts" in {
      reject [Cohort] {
        """{"state":"moving", "hosts":[1], "origin":[1], "target":[2]"}"""
      }}

    "reject an object with no origin" in {
      reject [Cohort] {
        """{"state":"moving", "origin":[], "target":[2]"}"""
      }}

    "reject an object with no target" in {
      reject [Cohort] {
        """{"state":"moving", "origin":[1], "target":[]"}"""
      }}}}
