/** *****************************************************************************
 * Copyright (c) 2019, 2020 lucendar.com.
 * All rights reserved.
 *
 * Contributors:
 * KwanKin Yau (alphax@vip.163.com) - initial API and implementation
 * ******************************************************************************/
package info.gratour.db

import java.sql.PreparedStatement
import java.util

import info.gratour.db.sql.{ExistsResultSetExtractor, StatementBinder}
import info.gratour.db.types.SQLDialect
import org.springframework.jdbc.core.{JdbcTemplate, PreparedStatementSetter}

object DbUtils {

  def mkArrayText(arr: Array[Int]): String = arr.mkString("{", ",", "}")

  def mkArrayText(arr: Array[Long]): String = arr.mkString("{", ",", "}")

  def mkArrayText(arr: Array[String]): String = arr.mkString("{", ",", "}")

  def mkArrayText[T](arr: Array[T], mapper: T => Object): String =
    if (mapper != null)
      arr.map(mapper(_)).mkString("{", ",", "}")
    else
      arr.mkString("{", ",", "}")

  def mkArrayText[T](coll: util.Collection[T], mapper: T => Object): String = {
    val str = new StringBuilder("{")
    var b = false;
    coll.forEach(o => {
      if (!b) {
        b = true;
      } else
        str.append(",")

      val s =
        if (mapper != null)
          mapper(o).toString
        else
          o.toString
      str.append(s)
    })

    str.append('}')
    str.toString()
  }

  def mkArrayText[T](coll: util.Collection[T]): String = mkArrayText(coll, null)

  /**
   *
   * @param s
   * @param sqlDialect
   * @return escaped string
   * @note Using SQLDialect.stringValueLiteral() instead
   */
  @Deprecated
  def stringValueLiteral(s: String)(implicit sqlDialect: SQLDialect): String = {
    sqlDialect.stringValueLiteral(s)
  }

  def exists(sql: String)(implicit template: JdbcTemplate): Boolean =
    template.query(sql, ExistsResultSetExtractor)

  def exists(sql: String, paramsBinding: StatementBinder => Unit)(implicit template: JdbcTemplate): Boolean =
    template.query(sql, new PreparedStatementSetter {
      override def setValues(ps: PreparedStatement): Unit = {
        val binder = StatementBinder(ps)
        paramsBinding.apply(binder)
      }
    }, ExistsResultSetExtractor)
}
