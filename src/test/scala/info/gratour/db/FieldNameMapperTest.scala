/** *****************************************************************************
 * Copyright (c) 2019, 2020 lucendar.com.
 * All rights reserved.
 *
 * Contributors:
 * KwanKin Yau (alphax@vip.163.com) - initial API and implementation
 * ******************************************************************************/
package info.gratour.db

import info.gratour.db.schema.FieldNameMapper

object FieldNameMapperTest {

  def main(args: Array[String]): Unit = {
    val mapper = new FieldNameMapper()
    val fieldName = mapper.toApiFieldName("f_rec_spd1")
    val colName = mapper.toFirstDbColumnName(fieldName)

    println(s"fieldName: $fieldName")
    println(s"colName: $colName")
  }
}
