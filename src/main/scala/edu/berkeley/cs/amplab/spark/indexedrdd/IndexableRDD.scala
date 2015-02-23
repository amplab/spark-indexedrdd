package edu.berkeley.cs.amplab.spark.indexedrdd

import edu.berkeley.cs.amplab.spark.indexedrdd.IndexableRDD.IndexableKey
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD._
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * An extension of the IndexedRDD which supports a wide variety of types
 * Created by mader on 2/23/15.
 *
 * @param ik the class for converting between keys and ids
 * @param baseRDD made with an IndexedRDD to make it easier to perform map, filter, etc
 * @tparam K the key of the dataset
 * @tparam V the value (can be anything
 */
class IndexableRDD[K: ClassTag, V: ClassTag](ik: IndexableKey[K],
                                             baseRDD: IndexedRDD[V]) extends Serializable {

  /**
   * An RDD that looks like it is supposed to
   */
  lazy val rawRDD = IndexableRDD.indexToKeys(ik,baseRDD)

  def get(key: K): Option[V] = multiget(Array(key)).get(key)

  def multiget(keys: Array[K]): Map[K,V] =
    baseRDD.multiget(keys.map(ik.toId)).map(kv => (ik.fromId(kv._1),kv._2))

  /**
   * Unconditionally updates the specified key to have the specified value. Returns a new
   * IndexableRDD that reflects the modification.
   */
  def put(k: K, v: V): IndexableRDD[K,V] = multiput(Map(k -> v))

  /**
   * Unconditionally updates the keys in `kvs` to their corresponding values. Returns a new
   * IndexedRDD that reflects the modification.
   */
  def multiput(kvs: Map[K, V]): IndexableRDD[K,V] =
    new IndexableRDD(ik,baseRDD.multiput(kvs.map(kv => (ik.toId(kv._1),kv._2))))

  /**
   * Updates the keys in `kvs` to their corresponding values, running `merge` on old and new values
   * if necessary. Returns a new IndexedRDD that reflects the modification.
   */
  def multiput(kvs: Map[K, V], merge: (K, V, V) => V): IndexableRDD[K,V] = {
    val idMerge = (a: Id, b: V, c: V) => merge(ik.fromId(a),b,c)
    new IndexableRDD(ik,baseRDD.multiput(kvs.map(kv => (ik.toId(kv._1),kv._2)),idMerge))
  }

  /**
   * Restricts the entries to those satisfying the given predicate. This operation preserves the
   * index for efficient joins with the original IndexedRDD and is implemented using soft deletions.
   *
   * @param pred the user defined predicate, which takes a tuple to conform to the `RDD[(K, V)]`
   * interface
   */
  def filter(pred: Tuple2[K, V] => Boolean): IndexableRDD[K,V] = {
    val idPred = (nt: Tuple2[Id,V]) => pred((ik.fromId(nt._1),nt._2))
    new IndexableRDD(ik,baseRDD.filter(idPred))
  }


  def cache() = new IndexableRDD(ik,baseRDD.cache)

  def collect() = rawRDD.collect()

  def count() = rawRDD.count()
}


object IndexableRDD extends Serializable {
  type Id = Long


  /**
   * The mapping between the key type and an id (Long)
   * @note it is essential that for all possible K in the program toId(fromId(idval)) == idval
   * @tparam K the type of the key (anything at all)
   *
   */
  trait IndexableKey[K] extends Serializable {
    def toId(key: K): Id
    def fromId(id: Id): K
  }

  def keysToIndex[K: ClassTag, V: ClassTag](ik: IndexableKey[K],elems: RDD[(K, V)]) =
    elems.mapPartitions(iter =>
      (iter.map(kv=> (ik.toId(kv._1),kv._2))),
      preservesPartitioning = true)
  def indexToKeys[K: ClassTag, V: ClassTag](ik: IndexableKey[K],elems: RDD[(Id, V)]) =
    elems.mapPartitions(iter =>
      (iter.map(kv=> (ik.fromId(kv._1),kv._2))),
      preservesPartitioning = true)
  /**
   * Constructs an IndexedRDD from an RDD of pairs, partitioning keys using a hash partitioner,
   * preserving the number of partitions of `elems`, and merging duplicate keys arbitrarily.
   */
  def apply[K: ClassTag, V: ClassTag](ik: IndexableKey[K],elems: RDD[(K, V)]): IndexableRDD[K,V] = {
    new IndexableRDD(ik, IndexedRDD(keysToIndex(ik,elems)))
  }

  /** Constructs an IndexedRDD from an RDD of pairs, merging duplicate keys arbitrarily. */
  def apply[K: ClassTag, V: ClassTag](ik: IndexableKey[K], elems: RDD[(K, V)],
                                      partitioner: Partitioner):
  IndexableRDD[K,V] = {
    val partitioned: RDD[(K, V)] = elems.partitionBy(partitioner)
    new IndexableRDD(ik, IndexedRDD(keysToIndex(ik,partitioned)))
  }

  /** Constructs an IndexedRDD from an RDD of pairs. */
  def apply[K: ClassTag, V: ClassTag](ik: IndexableKey[K],
                          elems: RDD[(K, V)], partitioner: Partitioner,
                          mergeValues: (V, V) => V): IndexableRDD[K,V] = {

    val partitioned: RDD[(K, V)] = elems.partitionBy(partitioner)
    new IndexableRDD(ik, IndexedRDD(keysToIndex(ik,partitioned),partitioner,mergeValues))
  }
}
