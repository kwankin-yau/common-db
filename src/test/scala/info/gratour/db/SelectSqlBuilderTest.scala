package info.gratour.db

import info.gratour.db.sql.{ParamValue, SelectRelation, Selection, SimpleSelectQuery, StringValue}
import org.junit.Test

class SelectSqlBuilderTest {

  @Test
  def testEnumToStr(): Unit = {

    val role = SelectRelation("t_role", "r")

    val sel = new Selection("user", "u")
      .project("id,name", "tenant_id,role_id")
      .project(role.col("name").as("role_name"))
      .innerJoin(role).on("role_id", "id")
//      .innerJoin("role_id", "t_role", "r", "id")
      .where("id", StringValue("1"))
      .where("name", ParamValue())
      .orderBy("name")

    println(sel)

    val sql = new SimpleSelectQuery("SELECT a FROM table t1")
      .where("b = ?")
      .where("c = ?")
      .whereNot("bo")
      .orderBy("d", true)
      .orderBy("e")

    println(sql)
  }
}
