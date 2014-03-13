package com.treode.async

class IterableLatch [A] private [async] (iter: Iterable [A]) {

  private def run [A, B] (iter: Iterable [A], cb: Callback [B]) (f: A => Async [B]): Unit =
    iter foreach (x => f (x) run (cb))

  def map [K, V] (f: A => Async [(K, V)]): Async [Map [K, V]] =
    Async.async { cb =>
      run [A, (K, V)] (iter, new MapLatch [K, V] (iter.size, cb)) (f)
    }

  def indexed [B] (f: ((A, Int)) => Async [B]) (implicit m: Manifest [B]): Async [Seq [B]] =
    Async.async { cb =>
      run [(A, Int), (Int, B)] (iter.zipWithIndex, new IndexedLatch (iter.size, cb)) {
        case (x, i) => f ((x, i)) map ((i, _))
      }
    }

  def seq [B] (f: A => Async [B]) (implicit m: Manifest [B]): Async [Seq [B]] =
    Async.async { cb =>
      run [A, B] (iter, new SeqLatch [B] (iter.size, cb)) (f)
    }

  def unit [B] (f: A => Async [B]): Async [Unit] =
    Async.async { cb =>
      run [A, B] (iter, new CountingLatch [B] (iter.size, cb)) (f)
    }}
