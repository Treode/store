package com.treode.store

import java.math.BigInteger
import scala.util.Random

import com.treode.cluster.HostId
import org.joda.time.Instant

case class TxId (id: Bytes, time: Instant) extends Ordered [TxId] {

  def compare (that: TxId): Int = {
    val r = id compare that.id
    if (r != 0) return r
    time.getMillis compare that.time.getMillis
  }

  override def toString: String =
    f"${id.toHexString}:${time.toString}"
}

object TxId extends Ordering [TxId] {

  private val _random = {
    import StorePicklers._
    tuple (hostId, fixedLong)
  }

  val MinValue = TxId (Bytes.MinValue, 0)

  def apply (id: Bytes, time: Long): TxId =
    TxId (id, new Instant (time))

  def random (host: HostId): TxId =
    TxId (Bytes (_random, (host, Random.nextLong)), Instant.now)

  def parse (s: String): Option [TxId] = {
    s.split (':') match {
      case Array (_id, _time) =>
        Some (new TxId (Bytes.fromHexString (_id), Instant.parse (_time)))
      case _ =>
        None
    }}

  def compare (x: TxId, y: TxId): Int =
    x compare y

  val pickler = {
    import StorePicklers._
    wrap (bytes, instant)
    .build (v => new TxId (v._1, v._2))
    .inspect (v => (v.id, v.time))
  }}
