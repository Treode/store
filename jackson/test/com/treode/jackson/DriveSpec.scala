package com.treode.jackson

import java.nio.file.Paths

import com.treode.disk.{DriveAttachment, DriveDigest, DriveGeometry}
import org.scalatest.FreeSpec

class DriveSpec extends FreeSpec with ModuleSpec {

  "Deserializing a drive attachment should" - {

    "work" in {
      accept (DriveAttachment (Paths.get ("/a"), DriveGeometry (30, 13, 1L<<40))) {
        """{"path":"/a","geometry":{"segmentBits":30,"blockBits":13,"diskBytes":1099511627776}}"""
      }}

    "reject an attachment with a bad path" in {
      reject [DriveAttachment] {
        """{"path": 1, geometry: {"segmentBits":30,"blockBits":13,"diskBytes":1099511627776}}"""
      }}

    "reject an attachment with bad geometry values" in {
      reject [DriveAttachment] {
        """{"path": "/a", geometry: {"segmentBits":-1,"blockBits":-1,"diskBytes":-1}}"""
      }}

    "reject an attachment with a bad geometry object" in {
      reject [DriveAttachment] {
        """{"path": "/a", geometry: 1}"""
      }}

    "reject an empty object" in {
      reject [DriveGeometry] ("{}")
    }

    "reject an integer" in {
      reject [DriveGeometry] ("1")
    }

    "reject a float" in {
      reject [DriveGeometry] ("1.0")
    }

    "reject an array" in {
      reject [DriveGeometry] ("[]")
    }}

  "Serializing a drive digest should" - {

    "work" in {
      assertString ("""{"path":"/a","geometry":{"segmentBits":30,"blockBits":13,"diskBytes":1099511627776},"allocated":1,"draining":false}""") {
        DriveDigest (Paths.get ("/a"), DriveGeometry (30, 13, 1L<<40), 1, false)
      }}}

  "Serializing drive geometry should" - {

    "work" in {
      assertString ("""{"segmentBits":30,"blockBits":13,"diskBytes":1099511627776}""") {
        DriveGeometry (30, 13, 1L<<40)
      }}}

  "Deserializing drive geometry should" - {

    "work" in {
      accept (DriveGeometry (30, 13, 1L<<40)) {
        """{"segmentBits":30,"blockBits":13,"diskBytes":1099511627776}"""
      }}

    "reject a geometry with bad values" in {
      reject [DriveGeometry] {
        """{"segmentBits":-1,"blockBits":-1,"diskBytes":-1}"""
      }}

    "reject an empty object" in {
      reject [DriveGeometry] ("{}")
    }

    "reject an integer" in {
      reject [DriveGeometry] ("1")
    }

    "reject a float" in {
      reject [DriveGeometry] ("1.0")
    }

    "reject an array" in {
      reject [DriveGeometry] ("[]")
    }}

  "Serializing a drive attachment should" - {

    "work" in {
      assertString ("""{"path":"/a","geometry":{"segmentBits":30,"blockBits":13,"diskBytes":1099511627776}}""") {
        DriveAttachment (Paths.get ("/a"), DriveGeometry (30, 13, 1L<<40))
      }}}}
