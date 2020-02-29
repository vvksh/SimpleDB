package xyz.viveks.simpledb.operators.aggregators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import xyz.viveks.simpledb.*;
import xyz.viveks.simpledb.operators.OpIterator;

/** Knows how to compute some aggregate over a set of StringFields. */
public class StringAggregator implements Aggregator {

  private static final long serialVersionUID = 1L;
  private int groupByField;
  private Type groupByFieldType;
  private int indexofAggregateField;
  private Op aggOp;
  private Map<Field, Integer> aggregatedResults;
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
   * @param op aggregation operator to use -- only supports COUNT
   * @throws IllegalArgumentException if what != COUNT
   */
  public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op op) {
    this.groupByField = gbfield;
    this.groupByFieldType = gbfieldtype;
    this.indexofAggregateField = afield;
    this.aggOp = op;
    hasGroupings = groupByField != NO_GROUPING;
    this.aggregatedResults = new HashMap<>();
    if (!op.equals(Op.COUNT)) {
      throw new IllegalArgumentException("String aggregator only supports COUNT");
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
    String newValue = ((StringField) tup.getField(indexofAggregateField)).getValue();
    int prevValue = this.aggregatedResults.getOrDefault(fieldToGroup, 0);
    aggregatedResults.put(fieldToGroup, prevValue + 1);
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
      if (hasGroupings) {
        tup.setField(0, entry.getKey());
        tup.setField(1, new IntField(entry.getValue()));
      } else {
        tup.setField(0, new IntField(entry.getValue()));
      }
      tuples.add(tup);
    }
    return tuples;
  }
}
