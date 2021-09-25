package info.gratour.db.sql

import java.util

import scala.util.control.Breaks

class Selection(val relation: SelectRelation) {

  def this(relationName: String) {
    this(SelectRelation(relationName))
  }

  def this(relationName: String, alias: String) {
    this(SelectRelation(relationName, alias))
  }

  private var _columnList: java.util.List[SelectColumn] = _
  private lazy val columnList: java.util.List[SelectColumn] = {
    if (_columnList == null)
      _columnList = new util.ArrayList[SelectColumn]()

    _columnList
  }

  private var _joinList: java.util.List[Join] = _
  private lazy val joinList: java.util.List[Join] = {
    if (_joinList == null)
      _joinList = new util.ArrayList[Join]()

    _joinList
  }


  private var _searchExprList: java.util.List[SearchExpr] = _
  private lazy val searchExprList: java.util.List[SearchExpr] = {
    if (_searchExprList == null)
      _searchExprList = new util.ArrayList[SearchExpr]()

    _searchExprList
  }

  private var _orderByList: java.util.List[OrderBy] = _
  private lazy val orderByList: java.util.List[OrderBy] = {
    if (_orderByList == null)
      _orderByList = new util.ArrayList[OrderBy]()

    _orderByList
  }


  def projectAll: Selection = {
    columnList.add(relation.col("*"))

    this
  }

  def project(columnName: String*): Selection = {
    columnName.foreach(c => {

      if (c.indexOf(",") >= 0) {
        c.split(",").foreach(
          c1 =>
            columnList.add(relation.col(c1))
        )
      } else
        columnList.add(relation.col(c))
    })


    this
  }


  def project(column: SelectColumn, columns: SelectColumn*): Selection = {
    columnList.add(column)

    columns.foreach(c => {
      columnList.add(c)
    })

    this
  }

  def col(columnName: String): SelectColumn = {
    var r: SelectColumn = null
    if (_columnList != null) {
      val loop = new Breaks
      loop.breakable {
        for (i <- 0 until _columnList.size()) {
          val c = _columnList.get(i)
          if (c.columnName.eq(columnName)) {
            r = c
            loop.break()
          }
        }
      }
    }

    if (r != null)
      r
    else
      SelectColumn(relation, columnName)
  }


  def join(join: Join): Selection = {
    joinList.add(join)

    this
  }

  def innerJoin(relation2: SelectRelation): JoinTable = {
    JoinTable(this, relation2, JoinType.INNER_JOIN)
  }
  def innerJoin(relation2Name: String, relation2Alias: String = null): JoinTable = {
    JoinTable(this, SelectRelation(relation2Name, relation2Alias), JoinType.INNER_JOIN)
  }

  def leftJoin(relation2: SelectRelation): JoinTable = {
    JoinTable(this, relation2, JoinType.LEFT_JOIN)
  }
  def leftJoin(relation2Name: String, relation2Alias: String = null): JoinTable = {
    JoinTable(this, SelectRelation(relation2Name, relation2Alias), JoinType.LEFT_JOIN)
  }

  def rightJoin(relation2: SelectRelation): JoinTable = {
    JoinTable(this, relation2, JoinType.RIGHT_JOIN)
  }
  def rightJoin(relation2Name: String, relation2Alias: String = null): JoinTable = {
    JoinTable(this, SelectRelation(relation2Name, relation2Alias), JoinType.RIGHT_JOIN)
  }

  def crossJoin(relation2: SelectRelation): JoinTable = {
    JoinTable(this, relation2, JoinType.CROSS_JOIN)
  }
  def crossJoin(relation2Name: String, relation2Alias: String = null): JoinTable = {
    JoinTable(this, SelectRelation(relation2Name, relation2Alias), JoinType.CROSS_JOIN)
  }

  def where(searchExpr: SearchExpr): Selection = {
    searchExprList.add(searchExpr)

    this
  }

  def where(columnName: String, valueExpr: ValueExpr): Selection = {
    where(SimplePredication(col(columnName), OpEqual, valueExpr))
  }

  def orderBy(columnName: String, desc: Boolean = false): Selection = {
    orderByList.add(OrderBy(col(columnName), desc))

    this
  }

  def orderBy(selectColumn: SelectColumn, desc: Boolean): Selection = {
    orderByList.add(OrderBy(selectColumn, desc))

    this
  }

  override def toString: String = {
    val str = new StringBuilder
    str.append("SELECT ")

    if (_columnList == null || _columnList.isEmpty)
      str.append("* ")
    else {
      var i = 0
      _columnList.forEach(c => {
        if (i > 0)
          str.append(", ")

        str.append(c.toString)
        i += 1
      })
      str.append(' ')
    }

    str.append("FROM ").append(relation.toString).append(' ')

    if (_joinList != null && !_joinList.isEmpty) {
      _joinList.forEach(j => {
        str.append(j.toString).append(' ')
      })
      str.append(' ')
    }

    if (_searchExprList != null && !_searchExprList.isEmpty) {
      str.append("WHERE ")
      var i = 0
      _searchExprList.forEach(expr => {
        if (i > 0)
          str.append(" AND ")

        str.append(expr.toString)
        i += 1
      })

      str.append(' ')
    }

    if (_orderByList != null && !_orderByList.isEmpty) {
      str.append("ORDER BY ")
      var i = 0
      _orderByList.forEach(o => {
        if (i > 0)
          str.append(", ")

        str.append(o.toString)
        i += 1
      })

      str.append(' ')
    }

    str.toString()
  }
}


case class SelectRelation(tableName: String, alias: String = null) {

  override def toString: String = {
    if (alias != null)
      tableName + " " + alias
    else
      tableName
  }

  def col(columnName: String): SelectColumn =
    SelectColumn(this, columnName)


}

case class SelectColumn(relation: SelectRelation, columnName: String, alias: String = null) extends ValueExpr {
  override def toString: String = {
    val c = if (alias == null)
      columnName
    else
      columnName + " AS " + alias

    if (relation.alias != null)
      relation.alias + "." + c
    else
      c
  }

  def as(alias: String): SelectColumn = SelectColumn(relation, columnName, alias)
}

object JoinType extends Enumeration {
  val LEFT_JOIN, RIGHT_JOIN, INNER_JOIN, CROSS_JOIN = Value

  implicit def toString(typ: JoinType.Value): String = {
    typ match {
      case LEFT_JOIN =>
        "LEFT JOIN"

      case RIGHT_JOIN =>
        "RIGHT JOIN"

      case INNER_JOIN =>
        "INNER JOIN"

      case CROSS_JOIN =>
        "CROSS JOIN"
    }
  }
}

case class JoinTable(selection: Selection, relation: SelectRelation, joinType: JoinType.Value) {

  def on(leftColName: String, rightColName: String): Selection = {
    selection.join(Join(selection.col(leftColName), joinType, relation.col(rightColName)))
    selection
  }

}

case class Join(col1: SelectColumn, joinType: JoinType.Value, col2: SelectColumn) {

  override def toString: String = {
    val str = new StringBuilder()
    str.append(JoinType.toString(joinType)).append(' ')
      .append(col2.relation.toString).append(" ON ")
      .append(col2.toString).append(" = ")
      .append(col1.toString)

    str.toString()
  }
}

case class OrderBy(column: SelectColumn, desc: Boolean = false) {

  override def toString: String = {
    val c = if (column.alias != null)
      column.alias
    else if (column.relation.alias != null)
      column.relation.alias + "." + column.columnName
    else
      column.columnName

    if (desc)
      c + " DESC"
    else
      c
  }
}

trait Operator

trait LogicalOperator extends Operator

object OpEqual extends LogicalOperator {
  override def toString: String = "="
}

object OpLess extends LogicalOperator {
  override def toString: String = "<"
}

object OpGreater extends LogicalOperator {
  override def toString: String = ">"
}

object OpLessEqual extends LogicalOperator {
  override def toString: String = "<="
}

object OpGreatEqual extends LogicalOperator {
  override def toString: String = ">="
}

object OpNotEqual extends LogicalOperator {
  override def toString: String = "<>"
}

object OpIs extends LogicalOperator {
  override def toString: String = "IS"
}

object OpIn extends LogicalOperator {
  override def toString: String = "IN"
}

object OpLike extends LogicalOperator {
  override def toString: String = "LIKE"
}

trait SearchExpr

trait ValueExpr

case class SimplePredication(column: SelectColumn, operator: LogicalOperator, value: ValueExpr) extends SearchExpr {

  override def toString: String = {
    val c = if (column.alias != null)
      column.alias
    else if (column.relation.alias != null)
      column.relation.alias + "." + column.columnName
    else
      column.columnName

    c + " " + operator.toString + " " + value.toString
  }

}

case class NotPredication(searchExpr: SearchExpr) extends SearchExpr {

  override def toString: String = {
    "NOT (" + searchExpr + ")"
  }
}

case class AndPredication(left: SearchExpr, right: SearchExpr) extends SearchExpr {
  override def toString: String = "(" + left + ") AND (" + right + ")"
}

case class OrPredication(left: SearchExpr, right: SearchExpr) extends SearchExpr {
  override def toString: String = "(" + left + ") OR (" + right + ")"
}

case class ExistsPredication(selection: Selection) extends SearchExpr {
  override def toString: String = {
    "EXISTS (" + selection.toString + ")"
  }
}

case class ColumnValue(column: SelectColumn) extends ValueExpr {
  override def toString: String = {
    if (column.alias != null)
      column.alias
    else if (column.relation.alias != null)
      column.relation.alias + "." + column.columnName
    else
      column.columnName
  }
}

case class StringValue(value: String) extends ValueExpr {

  override def toString: String = {
    "'" + value + "'"
  }

}

case class IntValue(value: Int) extends ValueExpr {

  override def toString: String = {
    value.toString
  }
}

case class BoolValue(value: Boolean) extends ValueExpr {

  override def toString: String = value.toString
}

case class ParamValue() extends ValueExpr {

  override def toString: String = "?"
}
