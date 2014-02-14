package com.treode.disk

import scala.collection.mutable.PriorityQueue

class MinimumCollector [A] (count: Int) {

  private val candidates = {
    import Ordering._
    new PriorityQueue [(Int, A)] () (Tuple2 (Int, by (_ => 0)))
  }

  def add (rank: Int, value: A) {
    candidates.enqueue ((Int.MaxValue - rank, value))
    if (candidates.size >= count)
      candidates.dequeue()
  }

  def result: Seq [A] =
    candidates.map (_._2) .toSeq
}
