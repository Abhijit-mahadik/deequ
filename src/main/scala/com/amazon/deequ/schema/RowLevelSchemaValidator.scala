/**
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not
 * use this file except in compliance with the License. A copy of the License
 * is located at
 *
 * http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package com.amazon.deequ.schema

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.storage.StorageLevel


sealed trait ColumnDefinition {
  def name: String

  def isNullable: Boolean

  def castExpression(): Column = {
    col(name)
  }
}

private[this] case class StringColumnDefinition(
                                                 name: String,
                                                 isNullable: Boolean = true,
                                                 minLength: Option[Int] = None,
                                                 maxLength: Option[Int] = None,
                                                 matches: Option[String] = None)
  extends ColumnDefinition

private[this] case class IntColumnDefinition(
                                              name: String,
                                              isNullable: Boolean = true,
                                              minValue: Option[Int] = None,
                                              maxValue: Option[Int] = None)
  extends ColumnDefinition {

  override def castExpression(): Column = {
    col(name).cast(IntegerType).as(name)
  }
}

private[this] case class ShortColumnDefinition(
                                              name: String,
                                              isNullable: Boolean = true,
                                              minValue: Option[Short] = None,
                                              maxValue: Option[Short] = None)
  extends ColumnDefinition {

  override def castExpression(): Column = {
    col(name).cast(ShortType).as(name)
  }
}

private[this] case class ByteColumnDefinition(
                                                name: String,
                                                isNullable: Boolean = true,
                                                minValue: Option[Byte] = None,
                                                maxValue: Option[Byte] = None)
  extends ColumnDefinition {

  override def castExpression(): Column = {
    col(name).cast(ByteType).as(name)
  }
}

private[this] case class DecimalColumnDefinition(
                                                  name: String,
                                                  precision: Int,
                                                  scale: Int,
                                                  isNullable: Boolean = true)
  extends ColumnDefinition {

  override def castExpression(): Column = {
    col(name).cast(DecimalType(precision, scale)).as(name)
  }
}

private[this] case class FloatColumnDefinition(
                                                name: String,
                                                isNullable: Boolean = true)
  extends ColumnDefinition {

  override def castExpression(): Column = {
    col(name).cast(FloatType).as(name)
  }
}

private[this] case class DoubleColumnDefinition(
                                                 name: String,
                                                 isNullable: Boolean = true)
  extends ColumnDefinition {

  override def castExpression(): Column = {
    col(name).cast(DoubleType).as(name)
  }
}

private[this] case class BooleanColumnDefinition(
                                                  name: String,
                                                  isNullable: Boolean = true)
  extends ColumnDefinition {

  override def castExpression(): Column = {
    col(name).cast(BooleanType).as(name)
  }
}

private[this] case class TimestampColumnDefinition(
                                                    name: String,
                                                    mask: String,
                                                    isNullable: Boolean = true)
  extends ColumnDefinition {

  override def castExpression(): Column = {
    unix_timestamp(col(name), mask).cast(TimestampType).as(name)
  }
}

private[this] case class DateColumnDefinition(
                                                    name: String,
                                                    mask: String,
                                                    isNullable: Boolean = true)
  extends ColumnDefinition {

  override def castExpression(): Column = {
    to_date(col(name), mask).cast(DateType).as(name)
  }
}


/** A simple schema definition for relational data in Andes */
case class RowLevelSchema(columnDefinitions: Seq[ColumnDefinition] = Seq.empty) {

  /**
   * Declare a textual column
   *
   * @param name       column name
   * @param isNullable are NULL values permitted?
   * @param minLength  minimum length of values
   * @param maxLength  maximum length of values
   * @param matches    regular expression which the column value must match
   * @return
   */
  def withStringColumn(
                        name: String,
                        isNullable: Boolean = true,
                        minLength: Option[Int] = None,
                        maxLength: Option[Int] = None,
                        matches: Option[String] = None)
  : RowLevelSchema = {

    RowLevelSchema(columnDefinitions :+ StringColumnDefinition(name, isNullable, minLength,
      maxLength, matches))
  }

  /**
   * Declare an integer column
   *
   * @param name       column name
   * @param isNullable are NULL values permitted?
   * @param minValue   minimum value
   * @param maxValue   maximum value
   * @return
   */
  def withIntColumn(
                     name: String,
                     isNullable: Boolean = true,
                     minValue: Option[Int] = None,
                     maxValue: Option[Int] = None)
  : RowLevelSchema = {

    RowLevelSchema(columnDefinitions :+ IntColumnDefinition(name, isNullable, minValue, maxValue))
  }

  /**
   * Declare an short column
   *
   * @param name       column name
   * @param isNullable are NULL values permitted?
   * @param minValue   minimum value
   * @param maxValue   maximum value
   * @return
   */
  def withShortColumn(
                     name: String,
                     isNullable: Boolean = true,
                     minValue: Option[Short] = None,
                     maxValue: Option[Short] = None)
  : RowLevelSchema = {

    RowLevelSchema(columnDefinitions :+ ShortColumnDefinition(name, isNullable, minValue, maxValue))
  }

  /**
   * Declare an short column
   *
   * @param name       column name
   * @param isNullable are NULL values permitted?
   * @param minValue   minimum value
   * @param maxValue   maximum value
   * @return
   */
  def withByteColumn(
                       name: String,
                       isNullable: Boolean = true,
                       minValue: Option[Byte] = None,
                       maxValue: Option[Byte] = None)
  : RowLevelSchema = {

    RowLevelSchema(columnDefinitions :+ ByteColumnDefinition(name, isNullable, minValue, maxValue))
  }

  /**
   * Declare an Float column
   *
   * @param name       column name
   * @param isNullable are NULL values permitted?
   * @return
   */
  def withFloatColumn(
                       name: String,
                       isNullable: Boolean = true)
  : RowLevelSchema = {
    RowLevelSchema(columnDefinitions :+ FloatColumnDefinition(name, isNullable))
  }

  /**
   * Declare an Double column
   *
   * @param name       column name
   * @param isNullable are NULL values permitted?
   * @return
   */
  def withDoubleColumn(
                        name: String,
                        isNullable: Boolean = true)
  : RowLevelSchema = {
    RowLevelSchema(columnDefinitions :+ DoubleColumnDefinition(name, isNullable))
  }

  /**
   * Declare an Boolean column
   *
   * @param name       column name
   * @param isNullable are NULL values permitted?
   * @return
   */
  def withBooleanColumn(
                         name: String,
                         isNullable: Boolean = true)
  : RowLevelSchema = {
    RowLevelSchema(columnDefinitions :+ BooleanColumnDefinition(name, isNullable))
  }



  /**
   * Declare a decimal column
   *
   * @param name       column name
   * @param precision  precision of values
   * @param scale      scale of values
   * @param isNullable are NULL values permitted?
   * @return
   */
  def withDecimalColumn(
                         name: String,
                         precision: Int,
                         scale: Int,
                         isNullable: Boolean = true)
  : RowLevelSchema = {

    RowLevelSchema(columnDefinitions :+ DecimalColumnDefinition(name, precision, scale, isNullable))
  }

  /**
   * Declare a timestamp column
   *
   * @param name       column name
   * @param mask       pattern for the timestamp
   * @param isNullable are NULL values permitted?
   * @return
   */
  def withTimestampColumn(
                           name: String,
                           mask: String,
                           isNullable: Boolean = true)
  : RowLevelSchema = {

    RowLevelSchema(columnDefinitions :+ TimestampColumnDefinition(name, mask, isNullable))
  }

  /**
   * Declare a date column
   *
   * @param name       column name
   * @param mask       pattern for the date
   * @param isNullable are NULL values permitted?
   * @return
   */
  def withDateColumn(
                           name: String,
                           mask: String,
                           isNullable: Boolean = true)
  : RowLevelSchema = {

    RowLevelSchema(columnDefinitions :+ DateColumnDefinition(name, mask, isNullable))
  }
}

/**
 * Result of enforcing a schema on textual data
 *
 * @param validRows      data frame holding the (casted) rows which conformed to the schema
 * @param numValidRows   number of rows which conformed to the schema
 * @param invalidRows    data frame holding the rows which did not conform to the schema
 * @param numInvalidRows number of rows which did not conform to the schema
 */
case class RowLevelSchemaValidationResult(
                                           validRows: DataFrame,
                                           numValidRows: Long,
                                           invalidRows: DataFrame,
                                           numInvalidRows: Long
                                         )

/** Enforce a schema on textual data */
object RowLevelSchemaValidator {

  private[this] val MATCHES_COLUMN = "validation_error"

  /**
   * Enforces a schema on textual data, filters out non-conforming columns and casts the result
   * to the requested types
   *
   * @param data a data frame holding the data to validate in string-typed columns
   * @param schema the schema to enforce
   * @param storageLevelForIntermediateResults the storage level for intermediate results
   *                                           (to control caching behavior)
   * @return results of schema enforcement
   */
  def validate(
                data: DataFrame,
                schema: RowLevelSchema,
                storageLevelForIntermediateResults: StorageLevel = StorageLevel.MEMORY_AND_DISK
              ): RowLevelSchemaValidationResult = {

    val dataWithMatches = data
      .withColumn(MATCHES_COLUMN, expr(toCNF(schema) + "\"\")"))


    dataWithMatches.persist(storageLevelForIntermediateResults)

    val validRows = extractAndCastValidRows(dataWithMatches, schema)
    val numValidRows = validRows.count()

    val invalidRows = dataWithMatches
      .where(col(MATCHES_COLUMN) =!= "")

    // TODO:.drop(MATCHES_COLUMN)

    val numInValidRows = invalidRows.count()

    dataWithMatches.unpersist(false)

    RowLevelSchemaValidationResult(validRows, numValidRows, invalidRows, numInValidRows)
  }

  private[this] def extractAndCastValidRows(
                                             dataWithMatches: DataFrame,
                                             schema: RowLevelSchema)
  : DataFrame = {

    val castExpressions = schema.columnDefinitions
      .map { colDef => colDef.name -> colDef.castExpression() }
      .toMap

    val projection = dataWithMatches.schema
      .map {
        _.name
      }
      .filter {
        _ != MATCHES_COLUMN
      }
      .map { name => castExpressions.getOrElse(name, col(name)) }

    dataWithMatches.select(projection: _*).where(col(MATCHES_COLUMN) === "")
  }

  private[this] def toCNF(schema: RowLevelSchema): String = {
    var nextCnf = "concat(\"\""
    schema.columnDefinitions.foldLeft("") { case (cnf, columnDefinition) =>
      nextCnf = if (cnf.eq(nextCnf)) cnf else nextCnf + ", "
      if (!columnDefinition.isNullable) {
        nextCnf = nextCnf.concat(
          lit(when(col(columnDefinition.name).isNotNull,
            lit("\"\"")
          ).otherwise("\"" + columnDefinition.name + ":NULL,\"")).toString())
        nextCnf = nextCnf + ", "

      }
      val colIsNull = col(columnDefinition.name).isNull

      columnDefinition match {

        case byteDef: ByteColumnDefinition =>

          val colAsByte = col(byteDef.name).cast(ByteType)

          /* null or successfully casted */
          nextCnf = nextCnf.concat(
            lit(when(
              colIsNull.or(colAsByte.isNotNull),
              lit("\"\"")
            ).otherwise("\"" + columnDefinition.name + ":D-TYPE,\"")).toString())
          nextCnf = nextCnf + ", "

          byteDef.minValue.foreach { value =>
            nextCnf = nextCnf.concat(
              lit(when(
                colIsNull.isNull.or(colAsByte.geq(value)),
                lit("\"\"")
              ).otherwise("\"" + columnDefinition.name + ":MIN,\"")).toString())
            nextCnf = nextCnf + ", "
          }

          byteDef.maxValue.foreach { value =>
            nextCnf = nextCnf.concat(
              lit(when(colIsNull.or(colAsByte.leq(value)), lit("\"\""))
                .otherwise("\"" + columnDefinition.name + ":MAX,\"")).toString())
            nextCnf = nextCnf + ", "
          }

        case shortDef: ShortColumnDefinition =>

          val colAsShort = col(shortDef.name).cast(ShortType)

          /* null or successfully casted */
          nextCnf = nextCnf.concat(
            lit(when(colIsNull.or(colAsShort.isNotNull), lit("\"\""))
              .otherwise("\"" + columnDefinition.name + ":D-TYPE,\"")).toString())
          nextCnf = nextCnf + ", "

          shortDef.minValue.foreach { value =>
            nextCnf = nextCnf.concat(
              lit(when(colIsNull.isNull.or(colAsShort.geq(value)), lit("\"\""))
                .otherwise("\"" + columnDefinition.name + ":MIN,\"")).toString())
            nextCnf = nextCnf + ", "
          }

          shortDef.maxValue.foreach { value =>
            nextCnf = nextCnf.concat(
              lit(when(colIsNull.or(colAsShort.leq(value)), lit("\"\""))
              .otherwise("\"" + columnDefinition.name + ":MAX,\"")).toString())
            nextCnf = nextCnf + ", "
          }

        case intDef: IntColumnDefinition =>

          val colAsInt = col(intDef.name).cast(IntegerType)

          /* null or successfully casted */
          nextCnf = nextCnf.concat(
            lit(when(colIsNull.or(colAsInt.isNotNull), lit("\"\""))
              .otherwise("\"" + columnDefinition.name + ":D-TYPE,\"")).toString())
          nextCnf = nextCnf + ", "

          intDef.minValue.foreach { value =>
            nextCnf = nextCnf.concat(
              lit(when(colIsNull.isNull.or(colAsInt.geq(value)), lit("\"\""))
                .otherwise("\"" + columnDefinition.name + ":MIN,\"")).toString())
            nextCnf = nextCnf + ", "
          }

          intDef.maxValue.foreach { value =>
            nextCnf = nextCnf.concat(
              lit(when(colIsNull.or(colAsInt.leq(value)), lit("\"\""))
                .otherwise("\"" + columnDefinition.name + ":MAX,\"")).toString())
            nextCnf = nextCnf + ", "
          }

        case decDef: DecimalColumnDefinition =>

          val decType = DataTypes.createDecimalType(decDef.precision, decDef.scale)
          nextCnf = nextCnf.concat(
            lit(when(colIsNull.or(col(decDef.name).cast(decType).isNotNull), lit("\"\""))
              .otherwise("\"" + columnDefinition.name + ":D-TYPE,\"")).toString())
          nextCnf = nextCnf + ", "
        case strDef: StringColumnDefinition =>

          strDef.minLength.foreach { value =>
            nextCnf = nextCnf.concat(
              lit(when(colIsNull.or(length(col(strDef.name)).geq(value)), lit("\"\""))
                .otherwise("\"" + columnDefinition.name + ":MIN,\"")).toString())
            nextCnf = nextCnf + ", "

          }

          strDef.maxLength.foreach { value =>
            nextCnf = nextCnf.concat(
              lit(when(colIsNull.or(length(col(strDef.name)).leq(value)), lit("\"\""))
                .otherwise("\"" + columnDefinition.name + ":MAX,\"")).toString())
            nextCnf = nextCnf + ", "
          }
          strDef.matches.foreach { regex =>
            var regexQuoted = "'" + regex.replace("\\", "\\\\") + "'"
            nextCnf = nextCnf.concat(
              lit(
                when(
                  colIsNull.or(regexp_extract(col(strDef.name), regexQuoted, 0).notEqual("\"\"")),
                  lit("\"\"")
                )
                  .otherwise("\"" + columnDefinition.name + ":PATTERN,\"")).toString()
            )
            nextCnf = nextCnf + ", "

          }
        case tsDef: TimestampColumnDefinition =>
          /* null or successfully casted */
          val maskQuoted = "'" + tsDef.mask + "'"
          nextCnf = nextCnf.concat(lit(when(colIsNull.or(unix_timestamp(col(tsDef.name), maskQuoted)
            .cast(TimestampType).isNotNull), lit("\"\""))
            .otherwise("\"" + columnDefinition.name + ":D-TYPE,\"")).toString())
          nextCnf = nextCnf + ", "
        case dateDef: DateColumnDefinition =>
          /* null or successfully casted */
          val maskQuoted = "'" + dateDef.mask + "'"
          nextCnf = nextCnf.concat(lit(when(colIsNull.or(to_date(col(dateDef.name), maskQuoted)
            .cast(DateType).isNotNull), lit("\"\""))
            .otherwise("\"" + columnDefinition.name + ":D-TYPE,\"")).toString())
          nextCnf = nextCnf + ", "
        case floatDef: FloatColumnDefinition =>
          /* null or successfully casted */
          val colAsFloat = col(floatDef.name).cast(FloatType)
          nextCnf = nextCnf.concat(lit(when(colIsNull.or(colAsFloat.isNotNull), lit("\"\""))
            .otherwise("\"" + columnDefinition.name + ":D-TYPE,\"")).toString())
          nextCnf = nextCnf + ", "
        case doubleDef: DoubleColumnDefinition =>
          /* null or successfully casted */
          val colAsDouble = col(doubleDef.name).cast(DoubleType)
          nextCnf = nextCnf.concat(lit(when(colIsNull.or(colAsDouble.isNotNull), lit("\"\""))
            .otherwise("\"" + columnDefinition.name + ":D-TYPE,\"")).toString())
          nextCnf = nextCnf + ", "
        case booleanDef: BooleanColumnDefinition =>
          /* null or successfully casted */
          val colAsBoolean = col(booleanDef.name).cast(BooleanType)
          nextCnf = nextCnf.concat(lit(when(colIsNull.or(colAsBoolean.isNotNull), lit("\"\""))
            .otherwise("\"" + columnDefinition.name + ":D-TYPE,\"")).toString())
          nextCnf = nextCnf + ", "

        case _ =>
      }

      println("Printing:" + nextCnf)
      nextCnf

    }

  }

}
