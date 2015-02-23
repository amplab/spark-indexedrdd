package edu.berkeley.cs.amplab.spark.indexedrdd


import edu.berkeley.cs.amplab.spark.indexedrdd.IndexableRDD.{Id, IndexableKey}
import org.apache.spark.SparkContext
import org.scalatest.FunSuite

import scala.collection.immutable.LongMap

/**
 * Created by mader on 2/23/15.
 */
class IndexableRDDSuite extends FunSuite with SharedSparkContext  {

  case class IdString(x: String)

  val idsKey = new IndexableKey[IdString] {
    override def toId(key: IdString): Id = key.x.toLong
    override def fromId(id: Id): IdString = IdString(id.toString)
  }

  case class Point3D(x: Int, y: Int, z: Int)

  val p3dKey = new IndexableKey[Point3D] {
    override def toId(key: Point3D): Id = 1000*key.z+100*key.y+key.x

    override def fromId(id: Id): Point3D =
      Point3D(
        Math.floor(id/1000).toInt,
        Math.floor((id % 1000)/100).toInt,
        (id%100).toInt
      )
  }
  def pairsP3D(sc: SparkContext, n: Int) = {
    IndexableRDD(p3dKey,sc.parallelize((0 to n).map(x => (Point3D(x,x,x), x)), 5))
  }
  def pairsIds(sc: SparkContext, n: Int) = {
    IndexableRDD(idsKey,sc.parallelize((0 to n).map(x => (IdString(x.toString()), x)), 5))
  }
  val n = 99

  test("get, multiget") {

    val ps = pairsP3D(sc, n).cache()
    assert(ps.get(-1L) === None, "Standard index based method for null elements")
    assert(ps.get(0L) === Some(0),"Get the first element")
    assert(ps.get(Point3D(0,0,0)) === Some(0), "Get the first point")
    assert(ps.get(Point3D(99,99,99)) === Some(99), "Get the last point")

  }

  test("filter on IndexableRDD") {
    val ps = pairsP3D(sc, n).cache()
    val evens = ps.filter(q => ((q._2 % 2) == 0)).cache()
    assert(evens.multiget(Array(-1L, 0L, 1L, 98L)) === LongMap(0L -> 0, 98L -> 98))
    assert(evens.get(97L) === None)
  }
}
