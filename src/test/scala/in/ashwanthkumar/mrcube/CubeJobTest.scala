package in.ashwanthkumar.mrcube

import com.twitter.scalding._
import scala.collection.mutable
import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers
import com.twitter.scalding.Csv

class CubeJob(args: Args) extends Job(args) {

  Csv(args("input"), fields = ('product, 'location, 'year, 'sales))
  .read
  .cubify[(String, String, String)](('product, 'location, 'year))
  .groupBy('product, 'location, 'year) { _.size('size).sum('sales) }
  .write(Csv(args("output")))
}

class CubeJobTest extends FunSuite with TupleConversions with FieldConversions with ShouldMatchers {

  test("should emit all combinations for a tuple") {

    def testInput = List(
      ("ipod", "miami", "2012", "200000")
    )

    def validate(buffer: mutable.Buffer[(String, String, String, String, String)]) {
      buffer.length should be(8)

      buffer should contain (("ipod", "miami", "2012", "1", "200000.0"))
      buffer should contain (("ipod", "miami", "null", "1", "200000.0"))
      buffer should contain (("ipod", "null", "null", "1", "200000.0"))
      buffer should contain (("ipod", "null", "2012", "1", "200000.0"))
      buffer should contain (("null", "null", "2012", "1", "200000.0"))
      buffer should contain (("null", "miami", "null", "1", "200000.0"))
      buffer should contain (("null", "miami", "2012", "1", "200000.0"))
      buffer should contain (("null", "null", "null", "1", "200000.0"))
    }

    new JobTest(new CubeJob(_))
    .arg("input", "input")
    .arg("output", "output")
    .source(Csv("input", fields = ('product, 'location, 'year, 'sales)), testInput)
    .source(Csv("input", fields = ('product1, 'location1, 'year1, 'sales1)), testInput)
    .sink(Csv("output"))(validate)
    .run
  }
}
