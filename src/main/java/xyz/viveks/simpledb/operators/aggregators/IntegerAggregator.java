package xyz.viveks.simpledb.operators.aggregators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import xyz.viveks.simpledb.*;
import xyz.viveks.simpledb.operators.OpIterator;

/** Knows how to compute some aggregate over a set of IntFields. */
public class IntegerAggregator implements Aggregator {

  private static final long serialVersionUID = 1L;
  private int groupByField;
  private Type groupByFieldType;
  private int indexofAggregateField;
  private Op aggOp;
  private Map<Field, Integer> aggregatedResults;

  // used in case of avg operation
  private Map<Field, Integer> countbyField;
  boolean hasGroupings;

  private static Field NO_GROUPING_FIELD = new IntField(NO_GROUPING);

  /**
   * Aggregate constructor
   *
   * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is
   *     no grouping
   * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no
   *     grouping
   * @param afield the 0-based index of the aggregate field in the tuple
   * @param op the aggregation operator
   */
  public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op op) {
    this.groupByField = gbfield;
    this.groupByFieldType = gbfieldtype;
    this.indexofAggregateField = afield;
    this.aggOp = op;
    hasGroupings = groupByField != NO_GROUPING;
    this.aggregatedResults = new HashMap<>();
    if (op.equals(Op.AVG)) {
      this.countbyField = new HashMap<>();
    }
  }

  /**
   * Merge a new tuple into the aggregate, grouping as indicated in the constructor
   *
   * @param tup the Tuple containing an aggregate field and a group-by field
   */
  public void mergeTupleIntoGroup(Tuple tup) {
    Field fieldToGroup = NO_GROUPING_FIELD;
    if (hasGroupings) {
      fieldToGroup = tup.getField(this.groupByField);
    }
    int newValue = ((IntField) tup.getField(indexofAggregateField)).getValue();
    switch (this.aggOp) {
      case AVG:
        {
          int prevSum = this.aggregatedResults.getOrDefault(fieldToGroup, 0);
          int prevCount = this.countbyField.getOrDefault(fieldToGroup, 0);
          int newSum =prevSum + newValue;
          aggregatedResults.put(fieldToGroup, newSum);
          countbyField.put(fieldToGroup, prevCount + 1);
          break;
        }
      case MAX:
        {
          int prevValue = this.aggregatedResults.getOrDefault(fieldToGroup, Integer.MIN_VALUE);
          aggregatedResults.put(fieldToGroup, Math.max(prevValue, newValue));
          break;
        }
      case MIN:
        {
          int prevValue = this.aggregatedResults.getOrDefault(fieldToGroup, Integer.MAX_VALUE);
          aggregatedResults.put(fieldToGroup, Math.min(prevValue, newValue));
          break;
        }
      case SUM:
        {
          int prevValue = this.aggregatedResults.getOrDefault(fieldToGroup, 0);
          aggregatedResults.put(fieldToGroup, prevValue + newValue);
          break;
        }
      case COUNT:
        {
          int prevValue = this.aggregatedResults.getOrDefault(fieldToGroup, 0);
          aggregatedResults.put(fieldToGroup, prevValue + 1);
          break;
        }
    }
  }

  /**
   * Create a OpIterator over group aggregate results.
   *
   * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal) if using group, or a
   *     single (aggregateVal) if no grouping. The aggregateVal is determined by the type of
   *     aggregate specified in the constructor.
   */
  public OpIterator iterator() {
    TupleDesc td;
    if (hasGroupings) {
      td = new TupleDesc(new Type[] {this.groupByFieldType, Type.INT_TYPE});
    } else {
      td = new TupleDesc(new Type[] {Type.INT_TYPE});
    }
    List<Tuple> tuples = getTuplesForAggregatedResults(td);
    return new TupleIterator(td, tuples);
  }

  private List<Tuple> getTuplesForAggregatedResults(TupleDesc td) {
    List<Tuple> tuples = new ArrayList<>();
    for (Map.Entry<Field, Integer> entry : aggregatedResults.entrySet()) {
      Tuple tup = new Tuple(td);
      int aggregatedValue = entry.getValue();
      if (this.aggOp == Op.AVG) {
        int count = this.countbyField.get(entry.getKey());
        aggregatedValue = aggregatedValue / count;
      }
      if (hasGroupings) {
        tup.setField(0, entry.getKey());
        tup.setField(1, new IntField(aggregatedValue));
      } else {
        tup.setField(0, new IntField(aggregatedValue));
      }
      tuples.add(tup);
    }
    return tuples;
  }


}
