/** *****************************************************************************
 * Copyright (c) 2019, 2021 lucendar.com.
 * All rights reserved.
 *
 * Contributors:
 * KwanKin Yau (alphax@vip.163.com) - initial API and implementation
 * ***************************************************************************** */
package info.gratour.db.sql

import java.sql.{Connection, PreparedStatement, ResultSet, Types}
import java.util
import java.util.function.Consumer

import info.gratour.db.sql.DbHelper.RowMapper2
import org.springframework.jdbc.core.{JdbcTemplate, PreparedStatementCallback, PreparedStatementCreator, PreparedStatementSetter, ResultSetExtractor, RowMapper}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

trait DbHelper {

  def template: JdbcTemplate

  def setConstraintsDeferred(): Unit = {
    template.execute("SET CONSTRAINTS ALL DEFERRED;")
  }

  def convert[T](accMapper: SequenceColumnAccessor => T): RowMapper[T] =
    (rs: ResultSet, _: Int) => {
      val acc = new SequenceColumnAccessor(rs)
      accMapper(acc)
    }


  def mapRow[T](rs: ResultSet, mapper: RowMapper2[T]): T = {
    val accessor = SequenceColumnAccessor(rs)
    mapper.map(accessor)
  }

  def mapRows[T](rs: ResultSet, mapper: RowMapper2[T]): java.util.List[T] = {
    val r = new util.ArrayList[T]()
    val accessor = new SequenceColumnAccessor(rs)
    while (rs.next()) {
      accessor.reset()

      r.add(mapper.map(accessor))
    }

    r
  }

  def preparedExec[T](sql: String, binding: StatementBinder => Unit, statementProcessor: PreparedStatement => T): T = {
    template.execute(
      new PreparedStatementCreator {
        override def createPreparedStatement(con: Connection): PreparedStatement = {
          val st = con.prepareStatement(sql)
          val binder = new StatementBinder(st)
          binding(binder)
          st
        }
      },
      new PreparedStatementCallback[T] {
        override def doInPreparedStatement(ps: PreparedStatement): T = {
          statementProcessor(ps)
        }
      }
    )
    //  using(template.prepareStatement(sql)) { st =>
    //    val binder = new StatementBinder(st)
    //    binding(binder)
    //    statementProcessor(st)
    //  }
  }

  type MultiBinder[T] = (T, StatementBinder) => Unit

  def preparedBatchUpdate[T](sql: String, values: Seq[T], multiBinder: MultiBinder[T]): Seq[Int] = {
    val r: ArrayBuffer[Int] = ArrayBuffer()

    template.execute(sql, new PreparedStatementCallback[Array[Int]] {
      override def doInPreparedStatement(ps: PreparedStatement): Array[Int] = {
        val binder = new StatementBinder(ps)
        val counts = new Array[Int](values.length)
        var index = 0;

        values.foreach(v => {
          ps.clearParameters()
          binder.idx.index = 0
          multiBinder(v, binder)
          val cnt = ps.executeUpdate()
          counts(index) = cnt
          index += 1;
        })

        counts
      }
    })

    r.toSeq
  }

  def executeQry(st: PreparedStatement): SequenceColumnAccessor = {
    val rs = st.executeQuery()
    new SequenceColumnAccessor(rs)
  }

}

object DbHelper {
  val TOTAL_ROW_COUNT_COLUMN_NAME = "_rc_"

  trait RowMapper2[T] {
    def map(accessor: SequenceColumnAccessor): T
  }

  def valuesToSqlPlaceHolders(values: Array[_]): String =
    if (values != null && !values.isEmpty)
      values.map(_ => "?").mkString(",")
    else
      null

  def tableExists(conn: Connection, tableName: String): Boolean = {
    val SELECT =
      """
            SELECT * FROM information_schema.tables
            WHERE table_type = 'BASE TABLE' AND table_name = '%s'
          """

    val sql = SELECT.format(tableName)

    DbHelper.existQuery(conn, sql)
  }


  def existQuery(conn: Connection, sql: String): Boolean = {
    val st = conn.createStatement()
    try {
      val rs = st.executeQuery(sql)
      try {
        rs.next()
      } finally {
        rs.close()
      }
    } finally {
      st.close()
    }
  }

  def executeDDL(conn: Connection, sql: String): Unit = {
    val st = conn.createStatement()
    try {
      st.execute(sql)
    } finally {
      st.close()
    }
  }

  def preparedQuerySingle[T](conn: Connection, sql: String, setter: PreparedStatementSetter, resultSetExtractor: ResultSetExtractor[T]): T = {
    val st = conn.prepareStatement(sql)
    try {
      setter.setValues(st)

      val rs = st.executeQuery()
      try {
        resultSetExtractor.extractData(rs)
      } finally {
        rs.close()
      }
    } finally {
      st.close()
    }
  }

  def preparedExec(conn: Connection, sql: String, setter: PreparedStatementSetter): Unit = {
    val st = conn.prepareStatement(sql)
    try {
      setter.setValues(st)

      st.execute()
    } finally {
      st.close()
    }
  }

  val BATCH_SIZE = 200

  def batchExec[T](conn: Connection, sql: String, args: util.Collection[T], setter: (PreparedStatement, T) => Unit): Unit = {
    val st = conn.prepareStatement(sql)
    try {
      var cnt = 0

      args.forEach(a => {
        st.clearParameters()
        setter.apply(st, a)
        st.addBatch()

        cnt += 1
        if (cnt == BATCH_SIZE) {
          st.executeBatch()
          cnt = 0
        }
      })

      if (cnt > 0)
        st.executeBatch()
    } finally {
      st.close()
    }
  }


  def preparedCall(conn: Connection, sql: String, setter: PreparedStatementSetter): Unit = {
    val st = conn.prepareCall(sql)
    try {
      setter.setValues(st)

      st.execute()
    } finally {
      st.close()
    }
  }

  /**
   * Execute query, map each row using `mapper`, and optional consumed by collector.
   *
   * @param conn
   * @param sql
   * @param mapper
   * @param collector collector consume the entity objected returned by mapper, optional, may be null.
   * @tparam T
   * @return
   */
  def executeQuery[T](conn: Connection, sql: String, mapper: RowMapper[T], collector: Consumer[T]): Int = {
    var cnt = 0;
    val st = conn.createStatement()
    try {
      val rs = st.executeQuery(sql)
      try {
        while (rs.next()) {
          cnt += 1
          val v = mapper.mapRow(rs, cnt)
          if (collector != null)
            collector.accept(v)
        }
      } finally {
        rs.close()
      }
    } finally {
      st.close()
    }

    cnt
  }

  /**
   * Execute query, map each row using `mapper`, and optional consumed by collector.
   *
   * @param conn
   * @param sql
   * @param mapper
   * @param collector collector consume the entity objected returned by mapper, optional, may be null.
   * @tparam T
   * @return
   */
  def executeQuery[T](conn: Connection, sql: String, mapper: RowMapper2[T], collector: Consumer[T]): Int = {
    var cnt = 0;
    val st = conn.createStatement()
    try {
      val rs = st.executeQuery(sql)
      try {
        val acc = SequenceColumnAccessor(rs)
        while (rs.next()) {
          cnt += 1

          acc.reset()
          val v = mapper.map(acc)
          if (collector != null)
            collector.accept(v)
        }
      } finally {
        rs.close()
      }
    } finally {
      st.close()
    }

    cnt
  }

  def valuesToSqlPlaceHolders(values: util.Collection[String]): String =
    if (values != null && !values.isEmpty) {

      values.asScala.map(_ => "?").mkString(",")
    } else
      null

  def strIdsParameterSetter(ids: util.Collection[String]): PreparedStatementSetter = (ps: PreparedStatement) => {
    var i = 1
    ids.asScala.foreach(id => {
      ps.setString(i, id)
      i += 1
    })
  }

  def strIdsParameterSetter(ids: String*): PreparedStatementSetter = (ps: PreparedStatement) => {
    var i = 1
    ids.foreach(id => {
      ps.setString(i, id)
      i += 1
    })
  }

  def strIdsParameterSetter(ids: Array[String]): PreparedStatementSetter = (ps: PreparedStatement) => {
    var i = 1
    ids.foreach(id => {
      ps.setString(i, id)
      i += 1
    })
  }

  def stringParameterSetter(value: String): PreparedStatementSetter = (ps: PreparedStatement) => {
    ps.setString(1, value)
  }

  def longParameterSetter(value: Long): PreparedStatementSetter = (ps: PreparedStatement) => {
    ps.setLong(1, value)
  }

  def longObjParameterSetter(value: java.lang.Long): PreparedStatementSetter = (ps: PreparedStatement) => {
    if (value != null)
      ps.setLong(1, value)
    else
      ps.setNull(1, Types.BIGINT)
  }

  def intParameterSetter(value: Int): PreparedStatementSetter = (ps: PreparedStatement) => {
    ps.setInt(1, value)
  }

  def intObjParameterSetter(value: java.lang.Integer): PreparedStatementSetter = (ps: PreparedStatement) => {
    if (value != null)
      ps.setInt(1, value)
    else
      ps.setNull(1, Types.INTEGER)
  }

  def optStringResultSetExtractor: ResultSetExtractor[Option[String]] = (rs: ResultSet) => {
    if (rs.next())
      Some(rs.getString(1))
    else
      None
  }

  def stringResultSetExtractor: ResultSetExtractor[String] = (rs: ResultSet) => {
    if (rs.next())
      rs.getString(1)
    else
      null
  }

  trait PreparedStatementBinder extends PreparedStatementSetter {

    def bindValues(binder: StatementBinder): Unit

    override def setValues(ps: PreparedStatement): Unit = {
      val binder = StatementBinder(ps)

      bindValues(binder)
    }
  }

  def parameterBinder(bind: StatementBinder => Unit): PreparedStatementSetter = new PreparedStatementBinder {
    override def bindValues(binder: StatementBinder): Unit = bind(binder)
  }


  def stringRowMapper: RowMapper[String] = (rs: ResultSet, _) => rs.getString(1)

  def recordExistsResultSetExtractor: ResultSetExtractor[java.lang.Boolean] = (rs: ResultSet) => rs.next()

  trait SeqColAccRowMapper[T] extends RowMapper[T] {
    def mapRow(acc: SequenceColumnAccessor): T

    override def mapRow(rs: ResultSet, rowNum: Int): T = {
      val acc = SequenceColumnAccessor(rs)
      mapRow(acc)
    }
  }

  def rowMapper[T](mapper: SequenceColumnAccessor => T): RowMapper[T] =
    new SeqColAccRowMapper[T] {
      override def mapRow(acc: SequenceColumnAccessor): T = {
        mapper(acc)
      }
    }
}
