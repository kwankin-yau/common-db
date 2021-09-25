/** *****************************************************************************
 * Copyright (c) 2019, 2020 lucendar.com.
 * All rights reserved.
 *
 * Contributors:
 * KwanKin Yau (alphax@vip.163.com) - initial API and implementation
 * ***************************************************************************** */
package info.gratour.db.types

import java.sql.Connection

import info.gratour.db.rest.SQLStringValueLiteral
import info.gratour.db.sql.DbHelper

trait SQLDialect extends SQLStringValueLiteral {

  def stringValueLiteral(s: String): String

  def tableExists(conn: Connection, tableName: String): Boolean
}

case object POSTGRESQL extends SQLDialect {

  override def stringValueLiteral(s: String): String =
    org.postgresql.core.Utils.escapeLiteral(null, s, true).toString

  override def tableExists(conn: Connection, tableName: String): Boolean = {
    val sql =
      """
              SELECT table_name FROM information_schema.tables
              WHERE  table_schema = 'public'
              AND    table_name   = '%s'
          """.format(stringValueLiteral(tableName))
    DbHelper.existQuery(conn, sql)
  }
}

case object SQLITE extends SQLDialect {
  override def stringValueLiteral(s: String): String =
    s.replace("\'", "\'\'")

  override def tableExists(conn: Connection, tableName: String): Boolean = {
    val sql =
      """
           SELECT name FROM sqlite_master WHERE type = 'table' AND name = '%s'
         """.format(stringValueLiteral(tableName))
    DbHelper.existQuery(conn, sql)
  }
}
