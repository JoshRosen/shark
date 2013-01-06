package shark.api.java

import spark._
import spark.api.java.function.{Function => JFunction}
import spark.api.java.JavaRDDLike
import spark.api.java.JavaRDD
import spark.RDD
import spark.storage.StorageLevel
import shark.execution.RowWrapper
import shark.execution.TableRDD
import shark.SharkContext

class JavaTableRDD(val rdd: TableRDD) extends JavaRDDLike[Any, JavaTableRDD] {

  override def wrapRDD(rdd: RDD[Any]): JavaTableRDD = new JavaTableRDD(rdd.asInstanceOf[TableRDD])

  // Common RDD functions

  override val classManifest: ClassManifest[Any] = implicitly[ClassManifest[Any]]

  /** Persist this RDD with the default storage level (`MEMORY_ONLY`). */
  def cache(): JavaTableRDD = wrapRDD(rdd.cache())

  /**
   * Set this RDD's storage level to persist its values across operations after the first time
   * it is computed. Can only be called once on each RDD.
   */
  def persist(newLevel: StorageLevel): JavaTableRDD = wrapRDD(rdd.persist(newLevel))

  // Transformations (return a new RDD)

  /**
   * Return a new RDD containing the distinct elements in this RDD.
   */
  def distinct(): JavaTableRDD = wrapRDD(rdd.distinct())

  /**
   * Return a new RDD containing the distinct elements in this RDD.
   */
  def distinct(numSplits: Int): JavaTableRDD = wrapRDD(rdd.distinct(numSplits))

  /**
   * Return a new RDD containing only the elements that satisfy a predicate.
   */
  def filter(f: JFunction[RowWrapper, java.lang.Boolean]): JavaTableRDD =
    wrapRDD(rdd.filter((x => f(x.asInstanceOf[RowWrapper]).booleanValue())))

  /**
   * Return a sampled subset of this RDD.
   */
  def sample(withReplacement: Boolean, fraction: Double, seed: Int): JavaTableRDD =
    wrapRDD(rdd.sample(withReplacement, fraction, seed))

  /**
   * Return the union of this RDD and another one. Any identical elements will appear multiple
   * times (use `.distinct()` to eliminate them).
   */
  def union(other: JavaTableRDD): JavaTableRDD = wrapRDD(rdd.union(other.rdd))

  // Shark-specific functions:

  def mapRows[T](f: JFunction[RowWrapper, T]): JavaRDD[T] =
    new JavaRDD(rdd.mapRows(f)(f.returnType()))(f.returnType())
}


