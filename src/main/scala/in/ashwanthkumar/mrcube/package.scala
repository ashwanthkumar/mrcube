package in.ashwanthkumar

import cascading.pipe.Pipe
import com.twitter.scalding.RichPipe

package object mrcube {

  implicit def pWithCubeOperations(r: Pipe): CubeOperations = new CubeOperations(r)

  implicit def rpWithCubeOperations(r: RichPipe): CubeOperations = new CubeOperations(r.pipe)

}
