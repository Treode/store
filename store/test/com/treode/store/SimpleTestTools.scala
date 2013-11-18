package com.treode.store

import com.treode.concurrent.Callback
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
      t.get (key, new Callback [Option [Bytes]] {
        def pass (value: Option [Bytes]) = expectResult (expected) (value)
        def fail (t: Throwable) = throw t
      })
    }}}

private object SimpleTestTools extends SimpleTestTools
