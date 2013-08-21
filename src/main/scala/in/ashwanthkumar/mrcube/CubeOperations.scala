package in.ashwanthkumar.mrcube

import com.twitter.scalding._
import scala.collection.mutable
import cascading.tuple.{Tuple, Fields}
import cascading.pipe.{Each, Pipe}
import cascading.operation.{FunctionCall, Function, BaseOperation}
import cascading.flow.FlowProcess
import com.twitter.scalding.Csv

class CubeOperations(val pipe: Pipe) {

  def cubify[T](fs: Fields, markerString: String = null)(implicit conv: TupleConverter[T]): Pipe = {
    conv.assertArityMatches(fs)
    new Each(pipe, fs, new CubifyFunction[T](fs, markerString), Fields.REPLACE)
  }
}

class CubifyFunction[T](fields: Fields, marker: String = null)
  extends BaseOperation[Any](fields) with Function[Any] {

  private[mrcube] def recursivelyCube(inputTuple: Tuple, newTuple: Tuple, fieldIndex: Int, outputTuples: List[Tuple]): List[Tuple] = {
    newTuple.set(fieldIndex, inputTuple.getObject(fieldIndex))
    val shouldAddTuple = inputTuple.size() - 1 == fieldIndex
    val output = shouldAddTuple match {
      case true => newTuple :: outputTuples
      case false => recursivelyCube(inputTuple, newTuple, fieldIndex + 1, outputTuples)
    }

    val newNewTuple = new Tuple(newTuple)
    newNewTuple.set(fieldIndex, String.valueOf(marker))
    val newOutput = shouldAddTuple match {
      case true => newNewTuple :: output
      case false => recursivelyCube(inputTuple, newNewTuple, fieldIndex + 1, output)
    }

    newOutput
  }

  def operate(flowProcess: FlowProcess[_], functionCall: FunctionCall[Any]) {
    recursivelyCube(functionCall.getArguments.getTuple,
      functionCall.getArguments.getTupleCopy, 0, List()) map functionCall.getOutputCollector.add
  }
}

