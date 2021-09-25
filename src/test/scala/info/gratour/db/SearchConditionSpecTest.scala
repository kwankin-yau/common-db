package info.gratour.db

import java.time.{LocalDate, LocalDateTime}

import info.gratour.common.error.ErrorWithCode
import info.gratour.db.rest.{ParsedSearchCondition, SearchCondition, SearchConditionSpec}
import info.gratour.db.schema.{FieldDataType, Predication}
import org.junit.Assert._
import org.junit.Test


class SearchConditionSpecTest {

  case class Condition(
                        spec: SearchConditionSpec,
                        cond: SearchCondition
                      ) {
    def check(): ParsedSearchCondition = {
      spec.check(cond, Array("f_a"))
    }
  }

  object Condition {
    def apply(fieldDataType: FieldDataType, textToSearch: String): Condition = {
      val spec = SearchConditionSpec("a", dataType = fieldDataType)

      val cond = SearchCondition("a", textToSearch)

      new Condition(spec, cond)
    }
  }

  @Test
  def testBool: Unit = {
    val cond = Condition(FieldDataType.BOOL, "true")
    val parsed = cond.check()
    assertEquals(parsed.predication, Predication.EQUAL)


    assertNull(parsed.predication2)
    assertEquals(parsed.parsedValues.length, 1)
    assertTrue(parsed.parsedValues(0).isInstanceOf[Boolean])
    assertTrue(parsed.parsedValues(0).asInstanceOf[Boolean])

  }

  @Test(expected = classOf[ErrorWithCode])
  def testBoolFailed: Unit = {
    val condBool = Condition(FieldDataType.BOOL, "a")
    condBool.check()
  }

  @Test
  def testInt: Unit = {
    val cond = Condition(FieldDataType.INT, "10")
    val parsed = cond.check()
    assertEquals(parsed.predication, Predication.EQUAL)
    assertNull(parsed.predication2)
    assertEquals(parsed.parsedValues.length, 1)
    assertTrue(parsed.parsedValues(0).isInstanceOf[Integer])
    assertEquals(parsed.parsedValues(0).asInstanceOf[Integer], Integer.valueOf(10))
  }

  @Test
  def testIntRange1: Unit = {
    val cond = Condition(FieldDataType.INT, "[10")
    val parsed = cond.check()
    assertEquals(parsed.predication, Predication.GREAT_EQUAL)
    assertNull(parsed.predication2)
    assertEquals(parsed.parsedValues.length, 1)
    assertTrue(parsed.parsedValues(0).isInstanceOf[Integer])
    assertEquals(parsed.parsedValues(0).asInstanceOf[Integer], Integer.valueOf(10))
  }

  @Test
  def testIntRange2: Unit = {
    val cond = Condition(FieldDataType.INT, "(10")
    val parsed = cond.check()
    assertEquals(parsed.predication, Predication.GREAT)
    assertNull(parsed.predication2)
    assertEquals(parsed.parsedValues.length, 1)
    assertTrue(parsed.parsedValues(0).isInstanceOf[Integer])
    assertEquals(parsed.parsedValues(0).asInstanceOf[Integer], Integer.valueOf(10))
  }

  @Test
  def testIntRange3: Unit = {
    val cond = Condition(FieldDataType.INT, "10]")
    val parsed = cond.check()
    assertEquals(parsed.predication, Predication.LESS_EQUAL)
    assertNull(parsed.predication2)
    assertEquals(parsed.parsedValues.length, 1)
    assertTrue(parsed.parsedValues(0).isInstanceOf[Integer])
    assertEquals(parsed.parsedValues(0).asInstanceOf[Integer], Integer.valueOf(10))
  }

  @Test
  def testIntRange4: Unit = {
    val cond = Condition(FieldDataType.INT, "10)")
    val parsed = cond.check()
    assertEquals(parsed.predication, Predication.LESS)
    assertNull(parsed.predication2)
    assertEquals(parsed.parsedValues.length, 1)
    assertTrue(parsed.parsedValues(0).isInstanceOf[Integer])
    assertEquals(parsed.parsedValues(0).asInstanceOf[Integer], Integer.valueOf(10))
  }

  @Test
  def testIntRange5: Unit = {
    val cond = Condition(FieldDataType.INT, "[10,11]")
    val parsed = cond.check()
    assertEquals(parsed.predication, Predication.GREAT_EQUAL)
    assertEquals(parsed.predication2, Predication.LESS_EQUAL)
    assertEquals(parsed.parsedValues.length, 2)
    assertTrue(parsed.parsedValues(0).isInstanceOf[Integer])
    assertEquals(parsed.parsedValues(0).asInstanceOf[Integer], Integer.valueOf(10))
    assertTrue(parsed.parsedValues(1).isInstanceOf[Integer])
    assertEquals(parsed.parsedValues(1).asInstanceOf[Integer], Integer.valueOf(11))
  }

  @Test(expected = classOf[ErrorWithCode])
  def testIntFailed: Unit = {
    val cond = Condition(FieldDataType.INT, "a")
    cond.check()
  }

  @Test(expected = classOf[ErrorWithCode])
  def testIntFailed2: Unit = {
    val cond = Condition(FieldDataType.INT, "[10]")
    cond.check()
  }

  @Test(expected = classOf[ErrorWithCode])
  def testIntFailed3: Unit = {
    val cond = Condition(FieldDataType.INT, "[]")
    cond.check()
  }

  @Test(expected = classOf[ErrorWithCode])
  def testIntFailed4: Unit = {
    val cond = Condition(FieldDataType.INT, "[@]")
    cond.check()
  }

  @Test(expected = classOf[ErrorWithCode])
  def testIntFailed5: Unit = {
    val cond = Condition(FieldDataType.INT, "[10,12]-")
    cond.check()
  }

  @Test
  def testString1: Unit = {
    val cond = Condition(FieldDataType.TEXT, "\\[10\\,12\\]-")
    val parsed = cond.check()
    assertEquals(parsed.predication, Predication.EQUAL)
    assertNull(parsed.predication2)
    assertEquals(parsed.parsedValues.length, 1)
    val s =parsed.parsedValues(0)
    println(s.getClass.getName)
    assertTrue(s.isInstanceOf[String])
    assertEquals(parsed.parsedValues(0).asInstanceOf[String], "[10,12]-")
  }

  @Test
  def testString2: Unit = {
    val cond = Condition(FieldDataType.TEXT, "\\%[10\\,12]-")
    val parsed = cond.check()
    assertEquals(parsed.predication, Predication.EQUAL)
    assertNull(parsed.predication2)
    assertEquals(parsed.parsedValues.length, 1)
    val v0 = parsed.parsedValues(0)
    assertTrue(v0.isInstanceOf[String])

    // the operator is not like, so leading backslash was removed
    assertEquals("\\%[10,12]-", v0.asInstanceOf[String])
  }

  @Test
  def testStringLike: Unit = {
    val cond = Condition(FieldDataType.TEXT, "%[10\\,12\\]-")
    val parsed = cond.check()
    assertEquals(parsed.predication, Predication.START_WITH)
    assertNull(parsed.predication2)
    assertEquals(parsed.parsedValues.length, 1)
    val v0 = parsed.parsedValues(0)
    assertTrue(v0.isInstanceOf[String])
    assertEquals(v0.asInstanceOf[String], "[10,12]-")
  }

  @Test
  def testStringLike2: Unit = {
    val cond = Condition(FieldDataType.TEXT, "%[10\\,12\\]\\%")
    val parsed = cond.check()
    assertEquals(parsed.predication, Predication.START_WITH)
    assertNull(parsed.predication2)
    assertEquals(parsed.parsedValues.length, 1)
    val v0 = parsed.parsedValues(0)
    assertTrue(v0.isInstanceOf[String])
    assertEquals(v0.asInstanceOf[String], "[10,12]\\%")
  }

  @Test
  def testStringLike3: Unit = {
    val cond = Condition(FieldDataType.TEXT, "%[10\\,12\\]\\%-")
    val parsed = cond.check()
    assertEquals(parsed.predication, Predication.START_WITH)
    assertNull(parsed.predication2)
    assertEquals(parsed.parsedValues.length, 1)
    val v0 = parsed.parsedValues(0)
    assertTrue(v0.isInstanceOf[String])
    assertEquals(v0.asInstanceOf[String], "[10,12]\\%-")
  }

  @Test
  def testStringLike4: Unit = {
    val cond = Condition(FieldDataType.TEXT, "%[10\\,12\\]%")
    val parsed = cond.check()
    assertEquals(parsed.predication, Predication.INCLUDE)
    assertNull(parsed.predication2)
    assertEquals(parsed.parsedValues.length, 1)
    val v0 = parsed.parsedValues(0)
    assertTrue(v0.isInstanceOf[String])
    assertEquals(v0.asInstanceOf[String], "[10,12]")
  }

  @Test
  def testStringLike5: Unit = {
    val cond = Condition(FieldDataType.TEXT, "[10\\%,12 \\\\]%")
    val parsed = cond.check()
    assertEquals(parsed.predication, Predication.END_WITH)
    assertNull(parsed.predication2)
    assertEquals(parsed.parsedValues.length, 1)
    val v0 = parsed.parsedValues(0)
    assertTrue(v0.isInstanceOf[String])
    assertEquals(v0.asInstanceOf[String], "[10\\%,12 \\]")
  }

  @Test(expected = classOf[ErrorWithCode])
  def testStringLikeFailed: Unit = {
    val cond = Condition(FieldDataType.TEXT, "[10\\,12\\]%-")
    val parsed = cond.check()
  }

  @Test
  def testDate: Unit = {
    val cond = Condition(FieldDataType.LOCAL_DATE, "2018-10-01")
    val parsed = cond.check()
    assertEquals(parsed.predication, Predication.EQUAL)
    assertNull(parsed.predication2)
    assertEquals(parsed.parsedValues.length, 1)
    val v0 = parsed.parsedValues(0)
    assertTrue(v0.isInstanceOf[LocalDate])

    // the operator is not like, so leading backslash was removed
    assertEquals(LocalDate.of(2018,10,1), v0.asInstanceOf[LocalDate])
  }

  @Test
  def testDate2: Unit = {
    val cond = Condition(FieldDataType.LOCAL_DATE, "[2018-10-01")
    val parsed = cond.check()
    assertEquals(parsed.predication, Predication.GREAT_EQUAL)
    assertNull(parsed.predication2)
    assertEquals(parsed.parsedValues.length, 1)
    val v0 = parsed.parsedValues(0)
    assertTrue(v0.isInstanceOf[LocalDate])

    // the operator is not like, so leading backslash was removed
    assertEquals(LocalDate.of(2018,10,1), v0.asInstanceOf[LocalDate])
  }

  @Test
  def testDate3: Unit = {
    val cond = Condition(FieldDataType.LOCAL_DATE, "2018-10-01)")
    val parsed = cond.check()
    assertEquals(parsed.predication, Predication.LESS)
    assertNull(parsed.predication2)
    assertEquals(parsed.parsedValues.length, 1)
    val v0 = parsed.parsedValues(0)
    assertTrue(v0.isInstanceOf[LocalDate])

    // the operator is not like, so leading backslash was removed
    assertEquals(LocalDate.of(2018,10,1), v0.asInstanceOf[LocalDate])
  }

  @Test
  def testDate4: Unit = {
    val cond = Condition(FieldDataType.LOCAL_DATE, "[2018-10-01, 2018-11-01)")
    val parsed = cond.check()
    assertEquals(parsed.predication, Predication.GREAT_EQUAL)
    assertEquals(parsed.predication2, Predication.LESS)
    assertEquals(parsed.parsedValues.length, 2)
    val v0 = parsed.parsedValues(0)
    assertTrue(v0.isInstanceOf[LocalDate])
    val v1 = parsed.parsedValues(1)
    assertTrue(v1.isInstanceOf[LocalDate])

    // the operator is not like, so leading backslash was removed
    assertEquals(LocalDate.of(2018,10,1), v0.asInstanceOf[LocalDate])
    assertEquals(LocalDate.of(2018,11,1), v1.asInstanceOf[LocalDate])
  }

  @Test(expected = classOf[ErrorWithCode])
  def testDateFailed: Unit = {
    val cond = Condition(FieldDataType.LOCAL_DATE, "[2018-10-01, 2018-11-01")
    val parsed = cond.check()
  }

  @Test
  def testDateTime: Unit = {
    val cond = Condition(FieldDataType.LOCAL_DATETIME, "2018-10-01T00:00:00")
    val parsed = cond.check()
    assertEquals(parsed.predication, Predication.EQUAL)
    assertNull(parsed.predication2)
    assertEquals(parsed.parsedValues.length, 1)
    val v0 = parsed.parsedValues(0)
    assertTrue(v0.isInstanceOf[LocalDateTime])

    // the operator is not like, so leading backslash was removed
    assertEquals(LocalDateTime.of(2018,10,1,0,0,0), v0.asInstanceOf[LocalDateTime])
  }

  @Test
  def testDateTime2: Unit = {
    val cond = Condition(FieldDataType.LOCAL_DATETIME, "[2018-10-01T00:00:00")
    val parsed = cond.check()
    assertEquals(parsed.predication, Predication.GREAT_EQUAL)
    assertNull(parsed.predication2)
    assertEquals(parsed.parsedValues.length, 1)
    val v0 = parsed.parsedValues(0)
    assertTrue(v0.isInstanceOf[LocalDateTime])

    // the operator is not like, so leading backslash was removed
    assertEquals(LocalDateTime.of(2018,10,1,0,0,0), v0.asInstanceOf[LocalDateTime])
  }

  @Test
  def testDateTime3: Unit = {
    val cond = Condition(FieldDataType.LOCAL_DATETIME, "2018-10-01T00:00:00)")
    val parsed = cond.check()
    assertEquals(parsed.predication, Predication.LESS)
    assertNull(parsed.predication2)
    assertEquals(parsed.parsedValues.length, 1)
    val v0 = parsed.parsedValues(0)
    assertTrue(v0.isInstanceOf[LocalDateTime])

    // the operator is not like, so leading backslash was removed
    assertEquals(LocalDateTime.of(2018,10,1,0,0,0), v0.asInstanceOf[LocalDateTime])
  }

  @Test
  def testDateTime4: Unit = {
    val cond = Condition(FieldDataType.LOCAL_DATETIME, "[2018-10-01T00:00:00, 2018-11-01T00:00:00)")
    val parsed = cond.check()
    assertEquals(parsed.predication, Predication.GREAT_EQUAL)
    assertEquals(parsed.predication2, Predication.LESS)
    assertEquals(parsed.parsedValues.length, 2)
    val v0 = parsed.parsedValues(0)
    assertTrue(v0.isInstanceOf[LocalDateTime])
    val v1 = parsed.parsedValues(1)
    assertTrue(v1.isInstanceOf[LocalDateTime])

    // the operator is not like, so leading backslash was removed
    assertEquals(LocalDateTime.of(2018,10,1,0,0,0), v0.asInstanceOf[LocalDateTime])
    assertEquals(LocalDateTime.of(2018,11,1,0,0,0), v1.asInstanceOf[LocalDateTime])
  }

  @Test(expected = classOf[ErrorWithCode])
  def testDateTimeFailed: Unit = {
    val cond = Condition(FieldDataType.LOCAL_DATETIME, "[2018-10-01, 2018-11-01")
    val parsed = cond.check()
  }


  @Test(expected = classOf[ErrorWithCode])
  def testDateTimeFailed2: Unit = {
    val cond = Condition(FieldDataType.LOCAL_DATETIME, "[2018-10-01T00:00:00, 2018-11-01T00:00:00")
    val parsed = cond.check()
  }

}
