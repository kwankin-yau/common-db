/** *****************************************************************************
 * Copyright (c) 2019, 2021 lucendar.com.
 * All rights reserved.
 *
 * Contributors:
 * KwanKin Yau (alphax@vip.163.com) - initial API and implementation
 * ***************************************************************************** */
package info.gratour.db.sql

import java.io.ByteArrayInputStream
import java.sql.{PreparedStatement, Timestamp, Types}
import java.time.{Instant, LocalDate, LocalTime, OffsetDateTime}

import com.typesafe.scalalogging.Logger
import info.gratour.common.Consts
import info.gratour.common.error.{ErrorWithCode, Errors}
import info.gratour.common.lang.Reflections
import info.gratour.common.types.{EpochMillis, IncIndex}
import info.gratour.common.utils.DateTimeUtils
import info.gratour.db.schema.FieldDataType

class StatementBinder(val st: PreparedStatement) {

  import StatementBinder.logger

  val idx: IncIndex = IncIndex()

  def setNull(sqlType: Int): Unit =
    st.setNull(idx.inc(), sqlType)


  def setBool(value: Boolean): Unit =
    st.setBoolean(idx.inc(), value)

  def setBoolObject(value: java.lang.Boolean): Unit =
    if (value != null)
      setBool(value)
    else
      setNull(Types.BOOLEAN)

  def setBoolOpt(value: Option[Boolean]): Unit =
    if (value.isDefined)
      setBool(value.get)
    else
      st.setNull(idx.inc(), Types.BOOLEAN)


  def setShort(value: Short): Unit =
    st.setShort(idx.inc(), value)

  def setShortObject(value: java.lang.Short): Unit =
    if (value != null)
      setShort(value)
    else
      setNull(Types.SMALLINT)

  def setShortOpt(value: Option[Short]): Unit =
    if (value.isDefined)
      setShort(value.get)
    else
      st.setNull(idx.inc(), Types.SMALLINT)


  def setInt(value: Int): Unit =
    st.setInt(idx.inc(), value)

  def setIntObject(value: java.lang.Integer): Unit =
    if (value != null)
      setInt(value)
    else
      setNull(Types.INTEGER)

  def setIntOpt(value: Option[Int]): Unit =
    if (value.isDefined)
      setInt(value.get)
    else
      setNull(Types.INTEGER)


  def setLong(value: Long): Unit =
    st.setLong(idx.inc(), value)

  def setLongObject(value: java.lang.Long): Unit =
    if (value != null)
      setLong(value)
    else
      setNull(Types.BIGINT)

  def setLongOpt(value: Option[Long]): Unit =
    if (value.isDefined)
      setLong(value.get)
    else
      setNull(Types.BIGINT)


  def setSingle(value: Float): Unit =
    st.setFloat(idx.inc(), value)

  def setSingleObject(value: java.lang.Float): Unit =
    if (value != null)
      setSingle(value)
    else
      setNull(Types.FLOAT)

  def setSingleOpt(value: Option[Float]): Unit =
    if (value.isDefined)
      setSingle(value.get)
    else
      setNull(Types.FLOAT)


  def setDouble(value: Double): Unit =
    st.setDouble(idx.inc(), value)

  def setDoubleObject(value: java.lang.Double): Unit =
    if (value != null)
      setDouble(value)
    else
      setNull(Types.DOUBLE)

  def setDoubleOpt(value: Option[Double]): Unit =
    if (value.isDefined)
      setDouble(value.get)
    else
      setNull(Types.DOUBLE)


  def setDecimal(value: java.math.BigDecimal): Unit =
    st.setBigDecimal(idx.inc(), value)

  def setString(value: String): Unit =
    st.setString(idx.inc(), value)

  def setLocalDate(value: LocalDate): Unit =
    if (value != null)
      st.setObject(idx.inc(), value)
    else
      setNull(Types.DATE)

  def setLocalTime(value: LocalTime): Unit =
    if (value != null)
      st.setObject(idx.inc(), value)
    else
      setNull(Types.TIME)

  def setOffsetDateTime(value: OffsetDateTime): Unit =
    if (value != null)
      st.setObject(idx.inc(), value)
    else
      setNull(Types.TIMESTAMP_WITH_TIMEZONE)

  def setTimestamp(value: Timestamp): Unit =
    if (value != null)
      st.setTimestamp(idx.inc(), value)
    else
      setNull(Types.TIMESTAMP_WITH_TIMEZONE)

  def setTimestamp(epochMillis: java.lang.Long): Unit =
    if (epochMillis != null)
      setTimestamp(new Timestamp(epochMillis))
    else
      setNull(Types.TIMESTAMP_WITH_TIMEZONE)

  def setOffsetDateTime(epochMilli: java.lang.Long): Unit =
    if (epochMilli != null)
      setOffsetDateTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(epochMilli), DateTimeUtils.DEFAULT_ZONE_ID))
    else
      setNull(Types.TIMESTAMP_WITH_TIMEZONE)

  def setEpochMillis(epochMillis: EpochMillis): Unit =
    if (epochMillis != null)
      setOffsetDateTime(epochMillis)
    else
      setNull(Types.TIMESTAMP_WITH_TIMEZONE)

  def setBinaryStream(stream: java.io.InputStream): Unit =
    if (stream != null)
      st.setBinaryStream(idx.inc(), stream)
    else
      setNull(Types.BINARY)

  def setBinaryStream(stream: java.io.InputStream, length: Long): Unit =
    if (stream != null)
      st.setBinaryStream(idx.inc(), stream, length)
    else
      setNull(Types.BINARY)

  def setBytes(bytes: Array[Byte], offset: Int, length: Int): Unit = {
    val in = new ByteArrayInputStream(bytes, offset, length)
    try {
      st.setBinaryStream(idx.inc(), in)
    } finally {
      in.close()
    }
  }

  def setBytes(bytes: Array[Byte]): Unit = {
    if (bytes != null)
      setBytes(bytes, 0, bytes.length)
    else
      setNull(Types.BINARY)
  }

  def setIntArray(intArray: Array[Int]): Unit = {
    if (intArray != null) {
      val arr = st.getConnection.createArrayOf("INTEGER", intArray.map(Integer.valueOf))
      st.setArray(idx.inc(), arr)
    } else
      setNull(Types.ARRAY)
  }

  import scala.language.experimental.macros
  import scala.reflect.runtime.universe._

  def setNullByClass(clazz: Class[_]): Unit = {
    clazz match {
      case Reflections.JBoolean =>
        setNull(Types.BOOLEAN)

      case Reflections.JByte | Reflections.JShort =>
        setNull(Types.SMALLINT)

      case Reflections.JInteger =>
        setNull(Types.INTEGER)

      case Reflections.JLong =>
        setNull(Types.BIGINT)

      case Reflections.JFloat =>
        setNull(Types.FLOAT)

      case Reflections.JDouble =>
        setNull(Types.DOUBLE)

      case Reflections.JBigDecimal =>
        setNull(Types.DECIMAL)

      case Reflections.JString =>
        setNull(Types.VARCHAR)

      case Reflections.JLocalDate =>
        setNull(Types.DATE)

      case Reflections.JLocalTime =>
        setNull(Types.TIME)

      case Reflections.JLocalDateTime =>
        setNull(Types.TIMESTAMP)

      case Reflections.JOffsetDateTime | Reflections.JEpochMillis =>
        setNull(Types.TIMESTAMP_WITH_TIMEZONE)

      case Reflections.JByteArray =>
        setNull(Types.BINARY)

      case Reflections.JInputStream =>
        setNull(Types.BINARY)

      case Reflections.JIntArray =>
        setNull(Types.ARRAY)

      case _ =>
        val msg = Errors.errorMessage(Errors.UNSUPPORTED_TYPE) + "\n" + clazz.getName
        logger.error(msg)
        throw new ErrorWithCode(Errors.UNSUPPORTED_TYPE, msg)
    }
  }

  def setNull(fieldDataType: FieldDataType): Unit = {
    val typ =
      fieldDataType match {
        case FieldDataType.BOOL => Types.BOOLEAN
        case FieldDataType.SMALL_INT => Types.SMALLINT
        case FieldDataType.INT => Types.INTEGER
        case FieldDataType.BIGINT => Types.BIGINT
        case FieldDataType.TEXT => Types.VARCHAR
        case FieldDataType.DECIMAL => Types.NUMERIC
        case FieldDataType.FLOAT => Types.REAL
        case FieldDataType.DOUBLE => Types.DOUBLE
        case FieldDataType.LOCAL_DATE => Types.DATE
        case FieldDataType.LOCAL_DATETIME => Types.TIMESTAMP
        case FieldDataType.OFFSET_DATETIME => Types.TIMESTAMP_WITH_TIMEZONE
        case FieldDataType.EPOCH_MILLIS => Types.TIMESTAMP_WITH_TIMEZONE
        case FieldDataType.BINARY => Types.BINARY
        case FieldDataType.INT_ARRAY => Types.ARRAY
      }

    setNull(typ)
  }

  def set[T](value: T, fieldDataType: FieldDataType)(implicit tag: TypeTag[T]): Unit = {
    value match {
      case boolean: Boolean =>
        setBool(boolean)
      case short: Short =>
        if (fieldDataType == FieldDataType.INT)
          setInt(short)
        else
          setShort(short)
      case int: Int =>
        if (fieldDataType == FieldDataType.SMALL_INT)
          setShort(int.toShort)
        else
          setInt(int)
      case l: Long =>
        if (fieldDataType == FieldDataType.OFFSET_DATETIME) {
          val odt = OffsetDateTime.ofInstant(Instant.ofEpochMilli(l), DateTimeUtils.DEFAULT_ZONE_ID)
          setOffsetDateTime(odt)
        } else if (fieldDataType == FieldDataType.EPOCH_MILLIS) {
          val ts = new Timestamp(l)
          setTimestamp(ts)
        } else
          setLong(l)
      case single: Float =>
        setSingle(single)
      case double: Double =>
        setDouble(double)
      case string: String =>
        if (fieldDataType == FieldDataType.BIGINT) {
          val l = string.toLong
          setLong(l)
        } else
          setString(string)
      case localDate: LocalDate =>
        setLocalDate(localDate)
      case localTime: LocalTime =>
        setLocalTime(localTime)
      case offsetDateTime: OffsetDateTime =>
        setOffsetDateTime(offsetDateTime)
      case epochMillis: EpochMillis =>
        setEpochMillis(epochMillis)

      case jbool: java.lang.Boolean =>
        if (jbool != null)
          setBool(jbool.booleanValue())
        else
          setNull(Types.BOOLEAN)

      case jbyte: java.lang.Byte =>
        if (jbyte != null)
          setShort(jbyte.shortValue())
        else
          setNull(Types.SMALLINT)

      case jshort: java.lang.Short =>
        if (jshort != null)
          setShort(jshort.shortValue())
        else
          setNull(Types.SMALLINT)

      case jint: java.lang.Integer =>
        if (jint != null)
          setInt(jint.intValue())
        else
          setNull(Types.INTEGER)

      case jlong: java.lang.Long =>
        if (jlong != null)
          setLong(jlong.longValue())
        else
          setNull(Types.BIGINT)

      case jsingle: java.lang.Float =>
        if (jsingle != null)
          setSingle(jsingle.floatValue())
        else
          setNull(Types.FLOAT)

      case jdouble: java.lang.Double =>
        if (jdouble != null)
          setDouble(jdouble.doubleValue())
        else
          setNull(Types.DOUBLE)

      case jdec: java.math.BigDecimal =>
        setDecimal(jdec)

      case jInputStream: java.io.InputStream =>
        setBinaryStream(jInputStream)

      case bytes: Array[Byte] =>
        setBytes(bytes)

      case intArr: Array[Int] =>
        setIntArray(intArr)

      case opt: Option[_] =>
        tag.tpe match {
          case TypeRef(_, _, args) =>
            val optArgType = args.head
            if (optArgType =:= info.gratour.common.Types.BoolType) {
              if (opt.isDefined)
                setBool(opt.get.asInstanceOf[Boolean])
              else
                setNull(Types.BOOLEAN)
            } else if (optArgType =:= info.gratour.common.Types.ByteType) {
              if (opt.isDefined)
                setShort(opt.get.asInstanceOf[Byte])
              else
                setNull(Types.SMALLINT)
            } else if (optArgType =:= info.gratour.common.Types.ShortType) {
              if (opt.isDefined)
                setShort(opt.get.asInstanceOf[Short])
              else
                setNull(Types.SMALLINT)
            } else if (optArgType =:= info.gratour.common.Types.IntType) {
              if (opt.isDefined)
                setInt(opt.get.asInstanceOf[Int])
              else
                setNull(Types.INTEGER)
            } else if (optArgType =:= info.gratour.common.Types.LongType) {
              if (opt.isDefined)
                setLong(opt.get.asInstanceOf[Long])
              else
                setNull(Types.BIGINT)
            } else if (optArgType =:= info.gratour.common.Types.FloatType) {
              if (opt.isDefined)
                setSingle(opt.get.asInstanceOf[Float])
              else
                setNull(Types.FLOAT)
            } else if (optArgType =:= info.gratour.common.Types.DoubleType) {
              if (opt.isDefined)
                setDouble(opt.get.asInstanceOf[Double])
              else
                setNull(Types.DOUBLE)
            } else if (optArgType =:= info.gratour.common.Types.InputStreamType) {
              if (opt.isDefined)
                setBinaryStream(opt.get.asInstanceOf[java.io.InputStream])
              else
                setNull(Types.BINARY)
            } else
              throw new ErrorWithCode(Errors.UNSUPPORTED_TYPE, "Unsupported element type: " + optArgType.toString)
        }


      case _ =>
        val tpe = tag.tpe

        if (tpe =:= info.gratour.common.Types.StringType)
          setNull(Types.VARCHAR)
        else if (tpe =:= info.gratour.common.Types.LocalDateType)
          setNull(Types.DATE)
        else if (tpe =:= info.gratour.common.Types.LocalTimeType)
          setNull(Types.TIME)
        else if (tpe =:= info.gratour.common.Types.OffsetDateTimeType)
          setNull(Types.TIMESTAMP_WITH_TIMEZONE)
        else if (tpe =:= info.gratour.common.Types.EpochMillisType)
          setNull(Types.TIMESTAMP_WITH_TIMEZONE)
        else if (tpe =:= info.gratour.common.Types.JBigDecimalType)
          setNull(Types.DECIMAL)
        else if (tpe =:= info.gratour.common.Types.JCharacterType)
          setNull(Types.CHAR)
        else if (tpe =:= info.gratour.common.Types.JBooleanType)
          setNull(Types.BOOLEAN)
        else if (tpe =:= info.gratour.common.Types.JIntegerType)
          setNull(Types.INTEGER)
        else if (tpe =:= info.gratour.common.Types.JShortType)
          setNull(Types.SMALLINT)
        else if (tpe =:= info.gratour.common.Types.JByteType)
          setNull(Types.SMALLINT)
        else if (tpe =:= info.gratour.common.Types.JFloatType)
          setNull(Types.FLOAT)
        else if (tpe =:= info.gratour.common.Types.JDoubleType)
          setNull(Types.DOUBLE)
        else if (tpe =:= info.gratour.common.Types.InputStreamType)
          setNull(Types.BINARY)
        else {
          if (fieldDataType == FieldDataType.TEXT) {
            if (value != null)
              setString(value.toString)
            else
              setString(null)
          } else
            throw new ErrorWithCode(Errors.UNSUPPORTED_TYPE)
        }
    }
  }

  def set[T](value: T)(implicit tag: TypeTag[T]): Unit = {
    value match {
      case boolean: Boolean =>
        setBool(boolean)
      case b: Byte =>
        setShort(b)
      case short: Short =>
        setShort(short)
      case int: Int =>
        setInt(int)
      case l: Long =>
        setLong(l)
      case single: Float =>
        setSingle(single)
      case double: Double =>
        setDouble(double)
      case string: String =>
        setString(string)

      case localDate: LocalDate =>
        setLocalDate(localDate)
      case localTime: LocalTime =>
        setLocalTime(localTime)
      case offsetDateTime: OffsetDateTime =>
        setOffsetDateTime(offsetDateTime)
      case epochMillis: EpochMillis =>
        setEpochMillis(epochMillis)

      case jbool: java.lang.Boolean =>
        if (jbool != null)
          setBool(jbool.booleanValue())
        else
          setNull(Types.BOOLEAN)

      case jbyte: java.lang.Byte =>
        if (jbyte != null)
          setShort(jbyte.shortValue())
        else
          setNull(Types.SMALLINT)
      case jshort: java.lang.Short =>
        if (jshort != null)
          setShort(jshort.shortValue())
        else
          setNull(Types.SMALLINT)

      case jint: java.lang.Integer =>
        if (jint != null)
          setInt(jint.intValue())
        else
          setNull(Types.INTEGER)

      case jlong: java.lang.Long =>
        if (jlong != null)
          setLong(jlong.longValue())
        else
          setNull(Types.BIGINT)

      case jsingle: java.lang.Float =>
        if (jsingle != null)
          setSingle(jsingle.floatValue())
        else
          setNull(Types.FLOAT)

      case jdouble: java.lang.Double =>
        if (jdouble != null)
          setDouble(jdouble.doubleValue())
        else
          setNull(Types.DOUBLE)

      case jdec: java.math.BigDecimal =>
        setDecimal(jdec)

      case stream: java.io.InputStream =>
        setBinaryStream(stream)

      case opt: Option[_] =>
        tag.tpe match {
          case TypeRef(_, _, args) =>
            val optArgType = args.head
            if (optArgType =:= info.gratour.common.Types.BoolType) {
              if (opt.isDefined)
                setBool(opt.get.asInstanceOf[Boolean])
              else
                setNull(Types.BOOLEAN)
            } else if (optArgType =:= info.gratour.common.Types.ByteType) {
              if (opt.isDefined)
                setShort(opt.get.asInstanceOf[Byte])
              else
                setNull(Types.SMALLINT)
            } else if (optArgType =:= info.gratour.common.Types.ShortType) {
              if (opt.isDefined)
                setShort(opt.get.asInstanceOf[Short])
              else
                setNull(Types.SMALLINT)
            } else if (optArgType =:= info.gratour.common.Types.IntType) {
              if (opt.isDefined)
                setInt(opt.get.asInstanceOf[Int])
              else
                setNull(Types.INTEGER)
            } else if (optArgType =:= info.gratour.common.Types.LongType) {
              if (opt.isDefined)
                setLong(opt.get.asInstanceOf[Long])
              else
                setNull(Types.BIGINT)
            } else if (optArgType =:= info.gratour.common.Types.FloatType) {
              if (opt.isDefined)
                setSingle(opt.get.asInstanceOf[Float])
              else
                setNull(Types.FLOAT)
            } else if (optArgType =:= info.gratour.common.Types.DoubleType) {
              if (opt.isDefined)
                setDouble(opt.get.asInstanceOf[Double])
              else
                setNull(Types.DOUBLE)
            } else if (optArgType =:= info.gratour.common.Types.InputStreamType) {
              if (opt.isDefined)
                setBinaryStream(opt.get.asInstanceOf[java.io.InputStream])
              else
                setNull(Types.BINARY)
            } else
              throw new ErrorWithCode(Errors.UNSUPPORTED_TYPE, "Unsupported element type: " + optArgType.toString)
        }


      case _ =>
        val tpe = tag.tpe

        if (tpe =:= info.gratour.common.Types.StringType)
          setNull(Types.VARCHAR)
        else if (tpe =:= info.gratour.common.Types.LocalDateType)
          setNull(Types.DATE)
        else if (tpe =:= info.gratour.common.Types.LocalTimeType)
          setNull(Types.TIME)
        else if (tpe =:= info.gratour.common.Types.OffsetDateTimeType)
          setNull(Types.TIMESTAMP_WITH_TIMEZONE)
        else if (tpe =:= info.gratour.common.Types.EpochMillisType)
          setNull(Types.TIMESTAMP_WITH_TIMEZONE)
        else if (tpe =:= info.gratour.common.Types.JBigDecimalType)
          setNull(Types.DECIMAL)
        else if (tpe =:= info.gratour.common.Types.JCharacterType)
          setNull(Types.CHAR)
        else if (tpe =:= info.gratour.common.Types.JBooleanType)
          setNull(Types.BOOLEAN)
        else if (tpe =:= info.gratour.common.Types.JIntegerType)
          setNull(Types.INTEGER)
        else if (tpe =:= info.gratour.common.Types.JShortType)
          setNull(Types.SMALLINT)
        else if (tpe =:= info.gratour.common.Types.JByteType)
          setNull(Types.SMALLINT)
        else if (tpe =:= info.gratour.common.Types.JFloatType)
          setNull(Types.FLOAT)
        else if (tpe =:= info.gratour.common.Types.JDoubleType)
          setNull(Types.DOUBLE)
        else if (tpe =:= info.gratour.common.Types.InputStreamType)
          setNull(Types.BINARY)
        else
          throw new ErrorWithCode(Errors.UNSUPPORTED_TYPE)
    }
  }

  def bind(values: Any*): Unit = macro StatementBinderMarcos.bind_impl


  def json(value: AnyRef): Unit = {
    if (value != null)
      setString(Consts.GSON.toJson(value))
    else
      setNull(Types.VARCHAR)
  }
}

object StatementBinder {
  private val logger = Logger[StatementBinder]

  def apply(ps: PreparedStatement): StatementBinder = new StatementBinder(ps)
}
