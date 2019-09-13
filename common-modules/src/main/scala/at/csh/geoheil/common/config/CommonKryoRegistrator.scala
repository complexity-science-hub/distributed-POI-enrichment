// Copyright (C) 2019 Georg Heiler
package at.csh.geoheil.common.config

import java.sql.Timestamp

import at.csh.geoheil.common.utils.Logging
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.sql.catalyst.expressions.UnsafeRow

import scala.reflect.ClassTag

class CommonKryoRegistrator extends KryoRegistrator with Logging {

  // inspiration how to register spark internal classes
  // https://github.com/bigdatagenomics/adam/blob/master/adam-core/src/main/scala/org/bdgenomics/adam/serialization/ADAMKryoRegistrator.scala
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[org.apache.spark.sql.types.ArrayType])
    kryo.register(classOf[org.apache.spark.sql.types.MapType])
    kryo.register(classOf[BigInt])
    kryo.register(classOf[Timestamp])
    kryo.register(classOf[Array[org.apache.spark.sql.types.MapType]])
    kryo.register(classOf[Array[org.apache.spark.sql.types.DataType]])
    kryo.register(classOf[Array[org.apache.spark.sql.types.Decimal]])
    kryo.register(classOf[Array[org.apache.spark.sql.types.DecimalType]])
    kryo.register(classOf[org.apache.spark.sql.types.DecimalType])
    kryo.register(classOf[org.apache.spark.sql.types.Decimal])
    kryo.register(classOf[scala.math.BigDecimal])
    kryo.register(classOf[org.apache.spark.sql.types.NullType])
    kryo.register(classOf[java.math.BigInteger])
    kryo.register(classOf[org.apache.spark.sql.types.Metadata])
    kryo.register(classOf[Array[org.apache.spark.sql.types.StructType]])
    kryo.register(classOf[org.apache.spark.sql.types.BooleanType])
    kryo.register(classOf[Array[org.apache.spark.sql.types.BooleanType]])
    kryo.register(classOf[org.apache.spark.sql.types.DateType])
    kryo.register(classOf[Array[org.apache.spark.sql.types.DateType]])
    kryo.register(classOf[org.apache.spark.sql.types.StructType])
    kryo.register(classOf[org.apache.spark.sql.types.StructField])
    kryo.register(classOf[Array[org.apache.spark.sql.types.StructField]])
    kryo.register(Class.forName("org.apache.spark.sql.types.BooleanType$"))
    kryo.register(
      Class.forName("org.apache.spark.sql.types.Decimal$DecimalAsIfIntegral$"))
    kryo.register(
      Class.forName("org.apache.spark.sql.types.Decimal$DecimalIsFractional$"))
    kryo.register(Class.forName("org.apache.spark.sql.types.DateType$"))
    kryo.register(Class.forName("org.apache.spark.sql.types.NullType$"))
    kryo.register(Class.forName("org.apache.spark.sql.types.TimestampType$"))
    kryo.register(
      Class.forName(
        "org.apache.spark.sql.execution.joins.UnsafeHashedRelation"))
    kryo.register(Class.forName("[[B"))
    kryo.register(Class.forName("org.apache.spark.sql.types.IntegerType$"))
    kryo.register(
      Class.forName("org.apache.spark.sql.catalyst.expressions.Descending$"))
    kryo.register(
      Class.forName("org.apache.spark.sql.catalyst.expressions.NullsLast$"))

    kryo.register(
      Class.forName("scala.reflect.ClassTag$$anon$1",
                    false,
                    getClass.getClassLoader))
    kryo.register(
      Class.forName("java.lang.Class", false, getClass.getClassLoader))

    kryo.register(
      classOf[org.apache.spark.sql.execution.command.PartitionStatistics])

    kryo.register(
      Class.forName("org.apache.spark.sql.execution.joins.LongToUnsafeRowMap"))
    kryo.register(Class.forName("org.apache.spark.sql.types.LongType$"))
    kryo.register(Class.forName("org.apache.spark.sql.types.StringType$"))

    kryo.register(
      Class.forName("org.apache.spark.sql.execution.joins.LongHashedRelation"))
    // java classes
    kryo.register(classOf[org.apache.spark.sql.catalyst.trees.Origin])
    kryo.register(classOf[org.apache.spark.sql.catalyst.util.QuantileSummaries])
    kryo.register(classOf[scala.Array[java.lang.Object]])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofRef[_]])
    kryo.register(Class.forName("scala.collection.mutable.ArraySeq"))
    kryo.register(Class.forName(
      "org.apache.spark.sql.execution.datasources.InMemoryFileIndex$SerializableFileStatus"))
    kryo.register(Class.forName(
      "org.apache.spark.sql.execution.datasources.InMemoryFileIndex$SerializableBlockLocation"))
    kryo.register(
      Class.forName("org.apache.spark.sql.execution.columnar.CachedBatch"))
    kryo.register(classOf[org.apache.spark.unsafe.types.UTF8String])
    kryo.register(Class.forName("scala.math.Ordering$$anon$4"))
    kryo.register(
      Class.forName("org.apache.spark.sql.catalyst.expressions.Ascending$"))
    kryo.register(
      Class.forName("org.apache.spark.sql.catalyst.expressions.NullsFirst$"))
    kryo.register(Class.forName("org.apache.spark.sql.types.DoubleType$"))
    kryo.register(classOf[
      org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering])
    kryo.register(ClassTag(Class.forName(
      "org.apache.spark.sql.execution.datasources.InMemoryFileIndex$SerializableBlockLocation")).wrap.runtimeClass)
    kryo.register(
      classOf[
        Array[org.apache.spark.sql.catalyst.util.QuantileSummaries.Stats]])
    kryo.register(
      classOf[org.apache.spark.sql.catalyst.util.QuantileSummaries.Stats])

    // spark internal classes
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofRef[_]])
    kryo.register(classOf[org.apache.spark.sql.catalyst.InternalRow])
    kryo.register(
      classOf[org.apache.spark.sql.catalyst.expressions.GenericInternalRow])
    kryo.register(classOf[Array[org.apache.spark.sql.catalyst.InternalRow]])
    kryo.register(
      classOf[Array[org.apache.spark.sql.catalyst.expressions.SortOrder]])
    kryo.register(
      classOf[Array[org.apache.spark.sql.catalyst.util.QuantileSummaries]])
    kryo.register(classOf[org.apache.spark.sql.catalyst.expressions.SortOrder])
    kryo.register(
      classOf[org.apache.spark.sql.catalyst.expressions.BoundReference])
    kryo.register(classOf[UnsafeRow])
    try {
      kryo.register(
        Class.forName(
          "org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"))
      kryo.register(Class.forName(
        "org.apache.spark.sql.execution.datasources.FileFormatWriter$WriteTaskResult"))
      kryo.register(
        Class.forName(
          "org.apache.spark.sql.execution.datasources.BasicWriteTaskStats"))
      kryo.register(
        Class.forName(
          "org.apache.spark.sql.execution.datasources.ExecutedWriteSummary"))
    } catch {
      case cnfe: java.lang.ClassNotFoundException => {
        logger.debug(
          "Did not find Spark internal class. This is expected for earlier Spark versions.")
      }
    }

    // scala specific classes:
    kryo.register(Class.forName("scala.collection.immutable.Set$EmptySet$"))

    // return unit to not get a warning
    ()
  }

}
