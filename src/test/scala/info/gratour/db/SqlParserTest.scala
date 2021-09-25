package info.gratour.db

import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.schema.Column
import net.sf.jsqlparser.statement.select.{PlainSelect, Select, SelectExpressionItem}
import org.junit.Test


class SqlParserTest {

  @Test
  def test(): Unit = {
    val sql = """
    SELECT u.user_id, u.tenant_id, t.tenant_name, u.username, u.g_admin, u.d_admin,
          u.create_uid, u2.username AS create_uname, u.create_time,
          u.update_uid, u2.username AS update_uname, u.update_time
    FROM t_user u
    LEFT JOIN t_user u2 ON u2.user_id = u.create_uid
    RIGHT JOIN t_user u3 ON u3.user_id = u.update_uid
    INNER JOIN t_tenant t on u.tenant_id = t.tenant_id
    """

    val select = CCJSqlParserUtil.parse(sql).asInstanceOf[Select]
    val plainSelect = select.getSelectBody.asInstanceOf[PlainSelect]
    plainSelect.getSelectItems.forEach {
      case sei: SelectExpressionItem =>
        sei.getExpression match {
          case c: Column =>
            if (sei.getAlias != null) {
              println(sei.getAlias.getName)
            } else
              println(c.getColumnName)
        }

      case _ =>
    }


  }

}
