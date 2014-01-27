package com.treode.store.simple

import com.treode.async.CallbackCaptor
import com.treode.store.Bytes
import org.scalatest.Assertions

import Assertions._

private trait SimpleTestTools {

  implicit class RichInt (v: Int) {
    def :: (k: Bytes): SimpleCell = SimpleCell (k, Some (Bytes (v)))
  }

  implicit class RichOption (v: Option [Bytes]) {
    def :: (k: Bytes): SimpleCell = SimpleCell (k, v)
  }}

private object SimpleTestTools extends SimpleTestTools
