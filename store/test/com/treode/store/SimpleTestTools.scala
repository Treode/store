package com.treode.store

import com.treode.async.CallbackCaptor
import org.scalatest.Assertions

import Assertions._

private trait SimpleTestTools {

  implicit class RichInt (v: Int) {
    def :: (k: Bytes): SimpleCell = SimpleCell (k, Some (Bytes (v)))
  }

  implicit class RichOption (v: Option [Bytes]) {
    def :: (k: Bytes): SimpleCell = SimpleCell (k, v)
  }

  implicit class RichSimpleTable (t: SimpleTable) {

    def getAndExpect (key: Bytes, expected: Option [Bytes]) {
      val cb = new CallbackCaptor [Option [Bytes]]
      t.get (key, cb)
      cb.passed
    }}}

private object SimpleTestTools extends SimpleTestTools
