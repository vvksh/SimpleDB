package xyz.viveks.simpledb;

import com.google.common.base.Preconditions;
import java.util.NoSuchElementException;

/** Filter is an operator that implements a relational select. */
public class Filter extends Operator {

  private static final long serialVersionUID = 1L;
  private Predicate predicate;
  private OpIterator childOperator;

  /**
   * Constructor accepts a predicate to apply and a child operator to read tuples to filter from.
   *
   * @param p The predicate to filter tuples with
   * @param child The child operator
   */
  public Filter(Predicate p, OpIterator child) {
    this.predicate = p;
    this.childOperator = child;
  }

  public Predicate getPredicate() {
    return predicate;
  }

  public TupleDesc getTupleDesc() {
    return childOperator.getTupleDesc();
  }

  public void open() throws DbException, NoSuchElementException, TransactionAbortedException {
    super.open();
    this.childOperator.open();
  }

  public void close() {
    this.childOperator.close();
    super.close();
  }

  public void rewind() throws DbException, TransactionAbortedException {
    close();
    open();
  }

  /**
   * AbstractDbIterator.readNext implementation. Iterates over tuples from the child operator,
   * applying the predicate to them and returning those that pass the predicate (i.e. for which the
   * Predicate.filter() returns true.)
   *
   * @return The next tuple that passes the filter, or null if there are no more tuples
   * @see Predicate#filter
   */
  protected Tuple fetchNext()
      throws NoSuchElementException, TransactionAbortedException, DbException {
    while (this.childOperator.hasNext()) {
      Tuple nextTuple = this.childOperator.next();
      if (predicate.filter(nextTuple)) {
        return nextTuple;
      }
    }
    return null;
  }

  @Override
  public OpIterator[] getChildren() {
    return new OpIterator[] {this.childOperator};
  }

  @Override
  public void setChildren(OpIterator[] children) {
    Preconditions.checkArgument(children.length == 1, "Filter accepts only one children");
    this.childOperator = children[0];
  }
}
