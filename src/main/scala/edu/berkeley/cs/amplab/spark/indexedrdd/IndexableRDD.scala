package edu.berkeley.cs.amplab.spark.indexedrdd

import edu.berkeley.cs.amplab.spark.indexedrdd.IndexableRDD.IndexableKey
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import scala.reflect.{classTag, ClassTag}

/**
 * An extension of the IndexedRDD which supports a wide variety of types
 * Created by mader on 2/23/15.
 */
class IndexableRDD[K: ClassTag, V: ClassTag](ik: IndexableKey[K],
                                             val rawRDD:
                                             RDD[(K,V)],
                                              partr: Partitioner)
  extends RDD[(K, V)](rawRDD.context, List(new OneToOneDependency(rawRDD))) {

  val baseRDD = IndexedRDD(rawRDD.map(kv => (ik.toId(kv._1),kv._2)))

  def get(key: K): Option[V] = multiget(Array(key)).get(key)

  def multiget(keys: Array[K]): Map[K,V] =
    baseRDD.multiget(keys.map(ik.toId)).map(kv => (ik.fromId(kv._1),kv._2))

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] =
    baseRDD.compute(split,context).map(kv => (ik.fromId(kv._1),kv._2))

  override protected def getPartitions: Array[Partition] = baseRDD.getPartitions
}

object IndexableRDD extends Serializable {
  type Id = Long

  trait IndexableKey[K] {
    def toId(key: K): Id
    def fromId(id: Id): K
  }


  /**
   * Constructs an IndexedRDD from an RDD of pairs, partitioning keys using a hash partitioner,
   * preserving the number of partitions of `elems`, and merging duplicate keys arbitrarily.
   */
  def apply[K: ClassTag, V: ClassTag](ik: IndexableKey[K],elems: RDD[(K, V)]): IndexableRDD[K,V] = {
    IndexableRDD(ik, elems, elems.partitioner.getOrElse(new HashPartitioner(elems.partitions.size)))
  }

  /** Constructs an IndexedRDD from an RDD of pairs, merging duplicate keys arbitrarily. */
  def apply[K: ClassTag, V: ClassTag](ik: IndexableKey[K], elems: RDD[(K, V)],
                                      partitioner: Partitioner):
  IndexableRDD[K,V] = {
    IndexableRDD(ik, elems, partitioner, (a, b) => b)
  }

  /** Constructs an IndexedRDD from an RDD of pairs. */
  def apply[K: ClassTag, V: ClassTag](ik: IndexableKey[K],
                          elems: RDD[(K, V)], partitioner: Partitioner,
                          mergeValues: (V, V) => V): IndexableRDD[K,V] = {
    val partitioned: RDD[(K, V)] = elems.partitionBy(partitioner)
    val partitions = partitioned.mapPartitions(
      iter => Iterator(IndexedRDDPartition(iter.map(kv => (ik.toId(kv._1),kv._2)), mergeValues)),
      preservesPartitioning = true)
    new IndexableRDD(ik,partitions)
  }
}
