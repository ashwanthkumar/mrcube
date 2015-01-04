package in.ashwanthkumar.mrcube

import com.twitter.scalding._
import scala.collection.mutable
import org.scalatest.FunSuite
import org.scalatest.Matchers

class CubifyJob(args: Args) extends Job(args) {

  Csv(args("input"), fields = ('product, 'location, 'year, 'sales))
    .read
    .cubify(('product, 'location, 'year))
    .groupBy('product, 'location, 'year) { _.size('size).sum[Double]('sales) }
    .write(Csv(args("output")))
}

class RollupJob(args: Args) extends Job(args) {
  Csv(args("input"), fields = ('product, 'location, 'year, 'sales))
    .read
    .rollup(('product, 'location, 'year))
    .groupBy('product, 'location, 'year) { _.size('size).sum[Double]('sales) }
    .write(Csv(args("output")))

}

class CubeOperationsTest extends FunSuite with FieldConversions with Matchers {

  test("should emit all combinations for a tuple in cubify") {

    def testInput = List(
      ("ipod", "miami", "2012", "200000")
    )

    def validate(buffer: mutable.Buffer[(String, String, String, String, String)]) {
      buffer.length should be(8)

      buffer should contain(("ipod", "miami", "2012", "1", "200000.0"))
      buffer should contain(("ipod", "miami", "null", "1", "200000.0"))
      buffer should contain(("ipod", "null", "null", "1", "200000.0"))
      buffer should contain(("ipod", "null", "2012", "1", "200000.0"))
      buffer should contain(("null", "null", "2012", "1", "200000.0"))
      buffer should contain(("null", "miami", "null", "1", "200000.0"))
      buffer should contain(("null", "miami", "2012", "1", "200000.0"))
      buffer should contain(("null", "null", "null", "1", "200000.0"))
    }

    new JobTest(new CubifyJob(_))
    .arg("input", "input")
    .arg("output", "output")
    .source(Csv("input", fields = ('product, 'location, 'year, 'sales)), testInput)
    .sink(Csv("output"))(validate)
    .run
  }

  test("should rollup the fields") {
    def testInput = List(
      ("ipod", "miami", "2012", "200000")
    )

    def validate(buffer: mutable.Buffer[(String, String, String, String, String)]) {
      buffer.length should be(4)

      buffer should contain(("ipod", "miami", "2012", "1", "200000.0"))
      buffer should contain(("null", "miami", "2012", "1", "200000.0"))
      buffer should contain(("null", "null", "2012", "1", "200000.0"))
      buffer should contain(("null", "null", "null", "1", "200000.0"))
    }

    new JobTest(new RollupJob(_))
    .arg("input", "input")
    .arg("output", "output")
    .source(Csv("input", fields = ('product, 'location, 'year, 'sales)), testInput)
    .sink(Csv("output"))(validate)
    .run

  }
}
