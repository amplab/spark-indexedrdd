package edu.berkeley.cs.amplab.spark.indexedrdd


import edu.berkeley.cs.amplab.spark.indexedrdd.IndexableRDD.{Id, IndexableKey}
import org.apache.spark.SparkContext
import org.scalatest.FunSuite

/**
 * Created by mader on 2/23/15.
 */
class IndexableRDDSuite extends FunSuite with SharedSparkContext  {
  import edu.berkeley.cs.amplab.spark.indexedrdd.IndexableRDDSuite._

  val n = 10

  test("get, multiget") {

    val ps = pairsP3D(sc, n).cache
    assert(ps.get(Point3D(0,0,0)) === Some(0), "Get the first point")
    assert(ps.get(Point3D(1,1,1)) === Some(1), "Get the second point")
    assert(ps.get(Point3D(9,9,9)) === Some(9), "Get one of the points at the end")
  }

  test("put, multiput") {
    val ps = pairsIds(sc, n).cache
    val plusOne = ps.put(IdString("-1"), -1)
    assert(plusOne.count === n+1+1,"Adding a single element that was not in the list before")
    assert(plusOne.get(IdString("-1")) === Some(-1),"Make sure the element is correct")

    val plusMany = ps.multiput(Map(IdString("0") -> 10, IdString("2") -> 10),
      (_,a: Int, b: Int) => a+b)
    assert(plusMany.count===n+1,"No new elements should have been added")
    assert(plusMany.get(IdString("0")) === Some(10),"New base value should be")
    assert(plusMany.get(IdString("1")) === Some(1),"New base value should be")
    assert(plusMany.get(IdString("2")) === Some(12),"New second value should be")
  }

  test("filter on value") {
    val ps = IndexableRDDSuite.pairsP3D(sc, n)
    val evens = ps.filter(q => ((q._2 % 2) == 0)).cache()

    assert(evens.get(Point3D(2,2,2)) === Some(2),"Contains an even point")
    assert(evens.get(Point3D(1,1,1)) === None,"Contains no odd points")
    assert(evens.count===Math.ceil((n+1.)/2).toInt,"Check the length")
  }
  test("filter on key") {
    val ps = IndexableRDDSuite.pairsP3D(sc, n)
    val limitVal = 5
    val lessThan5 = ps.filter(q => (q._1.x<=limitVal)).cache()

    assert(lessThan5.get(Point3D(1,1,1)) === Some(1),"Contains a point below "+limitVal)
    assert(lessThan5.get(Point3D(6,6,6)) === None,"Contains no point above "+limitVal)
    assert(lessThan5.count===limitVal+1,"Check the length")
  }


}


/**
 * Declared outside of test suite class to avoid closure capture (just like IndexedRDDSuite
 */
object IndexableRDDSuite {

  case class IdString(x: String)

  val idsKey = new IndexableKey[IdString] {
    override def toId(key: IdString): Id = key.x.toLong
    override def fromId(id: Id): IdString = IdString(id.toString)
  }

  case class Point3D(x: Int, y: Int, z: Int)


  /**
   * offers translation for positive Point3D between 0,0,0 and 99,99,99
   */
  val p3dKey = new IndexableKey[Point3D] {
    override def toId(key: Point3D): Id = (100*key.z+key.y)*100+key.x

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

  class SumFunction[K] extends Function3[K, Int, Int, Int] with Serializable {
    def apply(junk: K, a: Int, b: Int) = a + b
  }
}