package com.treode.store.disk2

import java.nio.file.{Path, Paths}
import com.treode.pickle.Picklers

private case class BootBlock (gen: Int, disks: Set [Path], roots: RootRegistry.Meta)

private object BootBlock {

  val pickle = {
    import Picklers._
    val path = wrap (string) build (Paths.get (_)) inspect (_.toString)
    val roots = RootRegistry.Meta.pickle
    wrap (int, set (path), roots)
    .build ((apply _).tupled)
    .inspect (v => (v.gen, v.disks, v.roots))
  }}
