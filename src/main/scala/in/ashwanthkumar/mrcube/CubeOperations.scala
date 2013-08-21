package in.ashwanthkumar.mrcube

import cascading.tuple.{Tuple, Fields}
import cascading.pipe.{Each, Pipe}
import cascading.operation.{FunctionCall, Function, BaseOperation}
import cascading.flow.FlowProcess
import scala.collection.JavaConversions.asScalaIterator

/**
 * Naive CubeOperations inspired from Apache Pig (https://issues.apache.org/jira/browse/PIG-2167)
 */
class CubeOperations(val pipe: Pipe) {
  /**
   * Produces a DataBag with all combinations of the argument tuple members
   * as in a data cube. Meaning, (a, b, c) will produce the following bag:
   * <pre>
   * { (a, b, c), (null, null, null), (a, b, null), (a, null, c),
   * (a, null, null), (null, b, c), (null, null, c), (null, b, null) }
   * </pre>
   *
   * <p>
   * The "all" marker is "null" by default, can be used changed with a
   * different value as well.
   * <p>
   */
  def cubify(fs: Fields, markerString: String = null): Pipe = {
    new Each(pipe, fs, new CubifyFunction(fs, markerString), Fields.REPLACE)
  }

  /**
   * Produces a DataBag with hierarchy of values (from the most detailed level of
   * aggregation to most general level of aggregation) of the specified dimensions
   * For example, (a, b, c) will produce the following bag:
   *
   * <pre>
   * { (a, b, c), (a, b, null), (a, null, null), (null, null, null) }
   * </pre>
   *
   */
  def rollup(fs: Fields, markerString: String = null): Pipe = {
    new Each(pipe, fs, new RollupFunction(fs, markerString), Fields.REPLACE)
  }
}

class CubifyFunction(fields: Fields, marker: String = null)
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

class RollupFunction(fields: Fields, marker: String = null)
  extends BaseOperation[Any](fields) with Function[Any] {

  private[mrcube] def iterativeRollup(inputTuple: Tuple) = {
    var currentTuple = new Tuple(inputTuple)
    inputTuple.iterator().zipWithIndex.foldLeft(List[Tuple]())((sofar, fieldWithIndex) => {
      val (_, index) = fieldWithIndex
      val inputCopy = new Tuple(currentTuple)
      inputCopy.set(index, String.valueOf(marker))
      currentTuple = inputCopy
      inputCopy :: sofar
    }) ++ List(inputTuple)
  }

  def operate(flowProcess: FlowProcess[_], functionCall: FunctionCall[Any]) {
    iterativeRollup(functionCall.getArguments.getTuple) map functionCall.getOutputCollector.add
  }

}

