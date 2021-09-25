/** *****************************************************************************
 * Copyright (c) 2019, 2020 lucendar.com.
 * All rights reserved.
 *
 * Contributors:
 * KwanKin Yau (alphax@vip.163.com) - initial API and implementation
 * ******************************************************************************/
package info.gratour.db.sql

import java.lang
import java.sql.ResultSet

import org.springframework.jdbc.core.ResultSetExtractor

object ExistsResultSetExtractor extends ResultSetExtractor[java.lang.Boolean] {
  override def extractData(rs: ResultSet): lang.Boolean =
    rs.next()
}
