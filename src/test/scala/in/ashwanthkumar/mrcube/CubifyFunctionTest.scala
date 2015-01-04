package in.ashwanthkumar.mrcube

import cascading.tuple.{Tuple, Fields}
import org.scalatest.{Matchers, FlatSpec}

class CubifyFunctionTest extends FlatSpec with Matchers {

  "Cubify" should "generate cubes for a tuple with 1 value" in {
    val cuber = new CubifyFunction(new Fields("foo"))
    val tuples = cuber.cubify(new Tuple("foo"))
    tuples should have length 2
    tuples should contain(new Tuple("foo"))
    tuples should contain(new Tuple("null"))
  }

  it should "generate cubes for a tuple with 2 values" in {
    val cuber = new CubifyFunction(new Fields("foo"))
    val tuples = cuber.cubify(new Tuple("foo", "bar"))
    tuples should have length 4
    tuples should contain(new Tuple("foo", "bar"))
    tuples should contain(new Tuple("null", "bar"))
    tuples should contain(new Tuple("foo", "null"))
    tuples should contain(new Tuple("null", "null"))
  }

  it should "generate cubes for a tuple with 2 values with custom marker" in {
    val cuber = new CubifyFunction(new Fields("foo"), "N/A")
    val tuples = cuber.cubify(new Tuple("foo", "bar"))
    tuples should have length 4
    tuples should contain(new Tuple("foo", "bar"))
    tuples should contain(new Tuple("N/A", "bar"))
    tuples should contain(new Tuple("foo", "N/A"))
    tuples should contain(new Tuple("N/A", "N/A"))
  }

}
