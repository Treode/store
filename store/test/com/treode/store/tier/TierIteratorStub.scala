package com.treode.store.tier

/** Iterate the values of the tier normally, that is without callbacks. */
private class TierIteratorStub (disk: DiskStub, pos: Long) extends Iterator [ValueEntry] {

  private var stack = List.empty [(IndexBlock, Int)]
  private var block: ValueBlock = null
  private var index = 0

  find (pos)

  private def find (pos: Long) {
    disk.get (pos) match {
      case b: IndexBlock =>
        val e = b.get (0)
        stack ::= (b, 0)
        find (e.pos)
      case b: ValueBlock =>
        block = b
        index = 0
      }}

  def hasNext: Boolean =
    index < block.size

  def next(): ValueEntry = {
    val entry = block.get (index)
    index += 1
    if (index == block.size && !stack.isEmpty) {
      var b = stack.head._1
      var i = stack.head._2 + 1
      stack = stack.tail
      while (i == b.size && !stack.isEmpty) {
        b = stack.head._1
        i = stack.head._2 + 1
        stack = stack.tail
      }
      if (i < b.size) {
        stack ::= (b, i)
        find (b.get (i) .pos)
      }}
    entry
  }}
