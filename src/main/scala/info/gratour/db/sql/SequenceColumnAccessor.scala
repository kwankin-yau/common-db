/** *****************************************************************************
 * Copyright (c) 2019, 2021 lucendar.com.
 * All rights reserved.
 *
 * Contributors:
 * KwanKin Yau (alphax@vip.163.com) - initial API and implementation
 * ***************************************************************************** */
package info.gratour.db.sql

import java.sql.{ResultSet, Timestamp}
import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime}

import com.google.gson.{Gson, GsonBuilder}
import info.gratour.common.types.{EpochMillis, IncIndex}

class SequenceColumnAccessor(val resultSet: ResultSet) {

  var rs: ResultSet = resultSet

  val colIndex: IncIndex = IncIndex()

  def next(): Boolean = {
    val r = rs.next()
    if (r)
      reset()

    r
  }

  def reset(): Unit = colIndex.index = 0

  def reset(resultSet: ResultSet): SequenceColumnAccessor = {
    rs = resultSet
    colIndex.index = 0
    this
  }

  def wasNull: Boolean = rs.wasNull()

  def str(): String = {
    rs.getString(colIndex.inc())
  }

  def small(): Short = {
    rs.getShort(colIndex.inc())
  }

  def smallObj(): java.lang.Short = {
    val r = rs.getShort(colIndex.inc())
    if (rs.wasNull())
      null
    else
      r
  }

  def smallOpt(): Option[Short] = {
    val r = rs.getShort(colIndex.inc())
    if (rs.wasNull())
      None
    else
      Some(r)
  }

  def int(): Int = {
    rs.getInt(colIndex.inc())
  }

  def intObj(): Integer = {
    val r = rs.getInt(colIndex.inc())
    if (rs.wasNull())
      null
    else
      r
  }

  def intOpt(): Option[Int] = {
    val r = rs.getInt(colIndex.inc())
    if (rs.wasNull())
      None
    else
      Some(r)
  }

  def long(): Long = {
    rs.getLong(colIndex.inc())
  }

  def longObj(): java.lang.Long = {
    val r = rs.getLong(colIndex.inc())
    if (rs.wasNull())
      null
    else
      r
  }

  def longOpt(): Option[Long] = {
    val r = rs.getLong(colIndex.inc())
    if (rs.wasNull())
      None
    else
      Some(r)
  }

  def bool(): Boolean =
    rs.getBoolean(colIndex.inc())

  def boolObj(): java.lang.Boolean = {
    val r = rs.getBoolean(colIndex.inc())
    if (rs.wasNull())
      null
    else
      r
  }


  def boolOpt(): Option[Boolean] = {
    val r = rs.getBoolean(colIndex.inc())
    if (rs.wasNull())
      None
    else
      Some(r)
  }

  def single(): Float =
    rs.getFloat(colIndex.inc())

  def singleObj(): java.lang.Float = {
    val r = rs.getFloat(colIndex.inc())
    if (rs.wasNull())
      null
    else
      r
  }

  def singleOpt(): Option[Float] = {
    val r = rs.getFloat(colIndex.inc())
    if (rs.wasNull())
      None
    else
      Some(r)
  }

  def double(): Double =
    rs.getDouble(colIndex.inc())

  def doubleObj(): java.lang.Double = {
    val r = rs.getDouble(colIndex.inc())
    if (rs.wasNull())
      null
    else
      r
  }

  def doubleOpt(): Option[Double] = {
    val r = rs.getDouble(colIndex.inc())
    if (rs.wasNull())
      None
    else
      Some(r)
  }

  def decimal(): java.math.BigDecimal =
    rs.getBigDecimal(colIndex.inc())

  def localDate(): LocalDate =
    rs.getObject(colIndex.inc(), classOf[LocalDate])

  def localTime(): LocalTime =
    rs.getObject(colIndex.inc(), classOf[LocalTime])

  def localDateTime(): LocalDateTime =
    rs.getObject(colIndex.inc(), classOf[LocalDateTime])

  def offsetDateTime(): OffsetDateTime =
    rs.getObject(colIndex.inc(), classOf[OffsetDateTime])

  def epochMillis(): EpochMillis = {
    val r = rs.getObject(colIndex.inc(), classOf[Timestamp])
    if (rs.wasNull())
      null
    else
      EpochMillis(r.getTime)
  }


  def json[T >: AnyRef]()(implicit m: Manifest[T]): T = {
    val s = rs.getString(colIndex.inc())
    if (rs.wasNull())
      null
    else
      SequenceColumnAccessor.GSON.fromJson(s, m.runtimeClass.asInstanceOf[Class[T]])
  }

  def byteArray(): Array[Byte] = {
    val str = rs.getBinaryStream(colIndex.inc())
    if (rs.wasNull())
      null
    else
      str.readAllBytes()
  }

  def intArray(): Array[Int] = {
    val arr = rs.getArray(colIndex.inc())
    if (rs.wasNull())
      null
    else
      arr.getArray().asInstanceOf[Array[Integer]].map(_.intValue())
  }
}

object SequenceColumnAccessor {
  def apply(rs: ResultSet): SequenceColumnAccessor = new SequenceColumnAccessor(rs)

  def apply(): SequenceColumnAccessor = new SequenceColumnAccessor(resultSet = null)


  val GSON: Gson = new GsonBuilder().create()
}
