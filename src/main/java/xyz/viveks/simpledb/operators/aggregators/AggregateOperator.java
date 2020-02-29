package xyz.viveks.simpledb.operators.aggregators;

import static xyz.viveks.simpledb.operators.aggregators.Aggregator.NO_GROUPING;

import com.google.common.base.Preconditions;
import java.util.*;
import xyz.viveks.simpledb.*;
import xyz.viveks.simpledb.operators.OpIterator;
import xyz.viveks.simpledb.operators.Operator;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max, min). Note that we only
 * support aggregates over a single column, grouped by a single column.
 */
public class AggregateOperator extends Operator {

  private static final long serialVersionUID = 1L;
  private OpIterator childOperator;
  private int indexOfAggregationField;
  private int groupByField;
  private Type groupByFieldType;
  private Aggregator.Op aggregationOp;
  private TupleDesc tupleDesc;
  private Aggregator aggregator;
  private OpIterator aggregateResultsIterator;

  /**
   * Constructor.
   *
   * <p>Implementation hint: depending on the type of afield, you will want to construct an {@link
   * xyz.viveks.simpledb.operators.aggregators.IntegerAggregator} or {@link
   * xyz.viveks.simpledb.operators.aggregators.StringAggregator} to help you with your
   * implementation of readNext().
   *
   * @param child The OpIterator that is feeding us tuples.
   * @param afield The column over which we are computing an aggregate.
   * @param gfield The column over which we are grouping the result, or -1 if there is no grouping
   * @param aop The aggregation operator to use
   */
  public AggregateOperator(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
    this.childOperator = child;
    this.indexOfAggregationField = afield;
    this.groupByField = gfield;
    this.aggregationOp = aop;
    this.groupByFieldType = null;
    if (this.groupByField != NO_GROUPING) {
      groupByFieldType = childOperator.getTupleDesc().getFieldType(groupByField);
    }
  }

  /**
   * @return If this aggregate is accompanied by a groupby, return the groupby field index in the
   *     <b>INPUT</b> tuples. If not, return {@link
   *     xyz.viveks.simpledb.operators.aggregators.Aggregator#NO_GROUPING}
   */
  public int groupField() {
    return groupByField;
  }

  /**
   * @return If this aggregate is accompanied by a group by, return the name of the groupby field in
   *     the <b>OUTPUT</b> tuples. If not, return null;
   */
  public String groupFieldName() {
    if (this.groupByField != NO_GROUPING) {
      return childOperator.getTupleDesc().getFieldName(this.groupByField);
    }
    return null;
  }

  /** @return the aggregate field */
  public int aggregateField() {
    return indexOfAggregationField;
  }

  /** @return return the name of the aggregate field in the <b>OUTPUT</b> tuples */
  public String aggregateFieldName() {
    return childOperator.getTupleDesc().getFieldName(this.indexOfAggregationField);
  }

  /** @return return the aggregate operator */
  public Aggregator.Op aggregateOp() {
    return this.aggregationOp;
  }

  public static String nameOfAggregatorOp(Aggregator.Op aop) {
    return aop.toString();
  }

  public void open() throws NoSuchElementException, DbException, TransactionAbortedException {
    super.open();
    this.childOperator.open();

    if (childOperator.getTupleDesc().getFieldType(indexOfAggregationField) == Type.INT_TYPE) {
      this.aggregator =
          new IntegerAggregator(
              groupByField, groupByFieldType, indexOfAggregationField, aggregationOp);
    } else if (childOperator.getTupleDesc().getFieldType(indexOfAggregationField)
        == Type.STRING_TYPE) {
      this.aggregator =
          new StringAggregator(
              groupByField, groupByFieldType, indexOfAggregationField, aggregationOp);
    }
    // calculate all aggregates
    while (childOperator.hasNext()) {
      Tuple nextTup = childOperator.next();
      this.aggregator.mergeTupleIntoGroup(nextTup);
    }
    this.aggregateResultsIterator = this.aggregator.iterator();
    this.aggregateResultsIterator.open();
  }

  /**
   * Returns the next tuple. If there is a group by field, then the first field is the field by
   * which we are grouping, and the second field is the result of computing the aggregate. If there
   * is no group by field, then the result tuple should contain one field representing the result of
   * the aggregate. Should return null if there are no more tuples.
   */
  protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    if (this.aggregateResultsIterator.hasNext()) {
      return this.aggregateResultsIterator.next();
    }
    return null;
  }

  public void rewind() throws DbException, TransactionAbortedException {
    close();
    open();
  }

  /**
   * Returns the TupleDesc of this Aggregate. If there is no group by field, this will have one
   * field - the aggregate column. If there is a group by field, the first field will be the group
   * by field, and the second will be the aggregate value column.
   *
   * <p>The name of an aggregate column should be informative. For example: "aggName(aop)
   * (child_td.getFieldName(afield))" where aop and afield are given in the constructor, and
   * child_td is the TupleDesc of the child iterator.
   */
  public TupleDesc getTupleDesc() {
    if (tupleDesc == null) {
      Type aggregateFieldType =
          childOperator.getTupleDesc().getFieldType(this.indexOfAggregationField);
      String aggregatedFieldName =
          childOperator.getTupleDesc().getFieldName(this.indexOfAggregationField);
      if (this.groupByField == NO_GROUPING) {
        tupleDesc =
            new TupleDesc(new Type[] {aggregateFieldType}, new String[] {aggregatedFieldName});
      } else {
        String groupbyFieldName = childOperator.getTupleDesc().getFieldName(groupByField);
        tupleDesc =
            new TupleDesc(
                new Type[] {groupByFieldType, aggregateFieldType},
                new String[] {groupbyFieldName, aggregatedFieldName});
      }
    }
    return tupleDesc;
  }

  public void close() {
    this.childOperator.close();
    this.aggregateResultsIterator.close();
    this.aggregateResultsIterator = null;
  }

  @Override
  public OpIterator[] getChildren() {
    return new OpIterator[] {childOperator};
  }

  @Override
  public void setChildren(OpIterator[] children) {
    Preconditions.checkArgument(children.length == 1, "Only accepts one child opiterator");
    this.childOperator = children[0];
  }
}
