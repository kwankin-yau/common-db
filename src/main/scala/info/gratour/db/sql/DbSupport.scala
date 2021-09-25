package info.gratour.db.sql


import java.lang
import java.sql.{CallableStatement, PreparedStatement, ResultSet}
import java.time.OffsetDateTime

import org.springframework.jdbc.core._

import scala.reflect.macros.whitebox

case class IdAndOffsetDateTime(id: String, offsetDateTime: OffsetDateTime) {
  def idToLong: Long = id.toLong
  def dateTimeToEpochMillis: Long = offsetDateTime.toInstant.toEpochMilli
}

class IdAndOffsetDateTimeRowMapper extends RowMapper[IdAndOffsetDateTime] {
  override def mapRow(rs: ResultSet, rowNum: Int): IdAndOffsetDateTime =
    IdAndOffsetDateTime(rs.getString(1), rs.getObject(2, classOf[OffsetDateTime]))
}

object IdAndOffsetDateTimeRowMapper {
  def apply(): IdAndOffsetDateTimeRowMapper = new IdAndOffsetDateTimeRowMapper
}

class IntRowMapper extends RowMapper[java.lang.Integer] {
  override def mapRow(rs: ResultSet, rowNum: Int): Integer = {
    val r = rs.getInt(1)
    if (rs.wasNull())
      null
    else
      r
  }
}

object IntRowMapper {
  def apply(): IntRowMapper = new IntRowMapper
}

class LongRowMapper extends RowMapper[java.lang.Long] {
  override def mapRow(rs: ResultSet, rowNum: Int): lang.Long = {
    val r = rs.getLong(1)
    if (rs.wasNull())
      null
    else
      r
  }
}

object LongRowMapper {
  def apply(): LongRowMapper = new LongRowMapper
}

class StrRowMapper extends RowMapper[String] {
  override def mapRow(rs: ResultSet, rowNum: Int): String = rs.getString(1)
}

object StrRowMapper {
  def apply(): StrRowMapper = new StrRowMapper
}

class CallableStmtBinder(override val st: CallableStatement) extends StatementBinder(st) {

  def registerOutParameter(sqlType: Int): Unit =
    st.asInstanceOf[CallableStatement].registerOutParameter(idx.inc(), sqlType)

  def getBool(colIndex: Int): Boolean =
    st.getBoolean(colIndex)

  def getBoolObject(colIndex: Int): java.lang.Boolean = {
    val r = st.getBoolean(colIndex)
    if (st.wasNull())
      null
    else
      r
  }

  def getInt(colIndex: Int): Int =
    st.getInt(colIndex)

  def getIntObject(colIndex: Int): Integer = {
    val r = st.getInt(colIndex)
    if (st.wasNull())
      null
    else
      r
  }

  def getLong(colIndex: Int): Long =
    st.getLong(colIndex)

  def getLongObject(colIndex: Int): java.lang.Long = {
    val r = st.getLong(colIndex)
    if (st.wasNull())
      null
    else
      r
  }

  def getString(colIndex: Int): String =
    st.getString(colIndex)

}

object CallableStmtBinder {
  def apply(cs: CallableStatement): CallableStmtBinder = new CallableStmtBinder(cs)
}

class StatementSetter(val wrapper: StatementBinder => Unit) extends PreparedStatementSetter {

  override def setValues(ps: PreparedStatement): Unit = {
    val binder = new StatementBinder(ps)
    wrapper(binder)
  }

}

object StatementSetter {
  def apply(wrapper: StatementBinder => Unit): StatementSetter = new StatementSetter(wrapper)
}

trait StatementBinderProcessor {
  def process(binder: StatementBinder): Unit
}


object StatementBinderMarcos {


  def bind_impl(c: whitebox.Context)(values: c.Expr[Any]*): c.Expr[Unit] = {
    import c.universe._


    val cl = c.prefix
    val list = values.map(expr => {
      q"""
        $cl.set($expr);
       """
    }).toList

    c.Expr[Unit](q"{..$list}")
  }

}

abstract class QueryResultObjectLoader[T](entryClass: Class[T]) {

  /**
   *
   * @param resultSet
   * @param entry
   * @return lastColumnIndex
   */
  def load(resultSet: ResultSet, entry: T): Int

  def toRowMapper: RowMapper[T] =
    (resultSet, _) => {
      val e = entryClass.getDeclaredConstructor().newInstance()
      load(resultSet, e)
      e
    }
}
