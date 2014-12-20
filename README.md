# IndexedRDD for Apache Spark

An efficient updatable key-value store for [Apache Spark](http://spark.apache.org).

IndexedRDD extends `RDD[(Long, V)]` by enforcing key uniqueness and pre-indexing the entries for efficient joins and point lookups, updates, and deletions. It is implemented by (1) hash-partitioning the entries by key, (2) maintaining a hash index within each partition, and (3) using purely functional (immutable and efficiently updatable) data structures to enable efficient modifications and deletions.

## Usage

Add the dependency to your SBT project by adding the following to `build.sbt`:

```scala
resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies += "edu.berkeley.cs.amplab" %% "spark-indexedrdd" % "0.1-SNAPSHOT"
```

Then use IndexedRDD as follows:

```scala
import edu.berkeley.cs.amplab.spark.IndexedRDD

// Create an RDD of key-value pairs with Long keys.
val rdd = sc.parallelize((1 to 1000000).map(x => (x.toLong, 0)))
// Construct an IndexedRDD from the pairs, hash-partitioning and indexing
// the entries.
val indexed = IndexedRDD(rdd).cache()

// Perform a point update.
val indexed2 = indexed.put(1234L, 10873).cache()
// Perform a point lookup. Note that the original IndexedRDD remains
// unmodified.
indexed2.get(1234L) // => Some(10873)
indexed.get(1234L) // => Some(0)

// Efficiently join derived IndexedRDD with original.
val indexed3 = indexed.innerJoin(indexed2) { (id, a, b) => b }.filter(_._2 != 0)
indexed3.collect // => Array((1234L, 10873))

// Perform insertions and deletions.
val indexed4 = indexed2.put(-100L, 111).delete(Array(998L, 999L)).cache()
indexed2.get(-100L) // => None
indexed4.get(-100L) // => Some(111)
indexed2.get(999L) // => Some(0)
indexed4.get(999L) // => None
```
