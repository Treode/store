package com.treode.jackson

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.FreeSpec

trait ModuleSpec {
  this: FreeSpec =>

  val mapper = new ObjectMapper()
  mapper.registerModule (DefaultTreodeModule)

  def assertString (expected: String) (input: Any): Unit =
    assertResult (expected) (mapper.writeValueAsString (input))

  def accept [A] (expected: A) (input: String) (implicit m: Manifest [A]): Unit =
    assertResult (expected) (mapper.readValue (input, m.runtimeClass))

  def reject [A] (input: String) (implicit m: Manifest [A]): Unit =
    intercept [JsonProcessingException] (mapper.readValue (input, m.runtimeClass))
}
