package in.ashwanthkumar.mrcube

import cascading.tuple.{Tuple, Fields}
import org.scalatest.{Matchers, FlatSpec}

class RollupFunctionTest extends FlatSpec with Matchers {

  "Rollup" should "generate rollup values for a tuple with 1 value" in {
    val rollup = new RollupFunction(new Fields("foo"))
    val tuples = rollup.iterativeRollup(new Tuple("foo"))
    tuples should have length 2
    tuples should contain(new Tuple("foo"))
    tuples should contain(new Tuple("null"))
  }

  it should "generate rollups for a tuple with 2 values" in {
    val rollup = new RollupFunction(new Fields("foo"))
    val tuples = rollup.iterativeRollup(new Tuple("foo", "bar"))
    tuples should have length 3
    tuples should contain(new Tuple("foo", "bar"))
    tuples should contain(new Tuple("null", "bar"))
    tuples should contain(new Tuple("null", "null"))
  }

  it should "generate rollups for a tuple with 2 values with custom marker" in {
    val rollup = new RollupFunction(new Fields("foo"), "N/A")
    val tuples = rollup.iterativeRollup(new Tuple("foo", "bar"))
    tuples should have length 3
    tuples should contain(new Tuple("foo", "bar"))
    tuples should contain(new Tuple("N/A", "bar"))
    tuples should contain(new Tuple("N/A", "N/A"))
  }
}
