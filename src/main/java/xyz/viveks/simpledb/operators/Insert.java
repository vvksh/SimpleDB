package xyz.viveks.simpledb.operators;

import com.google.common.base.Preconditions;
import java.io.IOException;
import xyz.viveks.simpledb.*;

/** Inserts tuples read from the child operator into the tableId specified in the constructor */
public class Insert extends Operator {

  private static final long serialVersionUID = 1L;
  private TransactionId tid;
  private OpIterator childOperator;
  private int tableId;
  private TupleDesc td;
  private boolean executed;

  /**
   * Constructor.
   *
   * @param tid The transaction running the insert.
   * @param child The child operator from which to read tuples to be inserted.
   * @param tableId The table in which to insert tuples.
   * @throws DbException if TupleDesc of child differs from table into which we are to insert.
   */
  public Insert(TransactionId tid, OpIterator child, int tableId) throws DbException {
    this.tid = tid;
    this.childOperator = child;
    this.tableId = tableId;
    this.td = new TupleDesc(new Type[] {Type.INT_TYPE});

    if (!this.childOperator.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId))) {
      throw new DbException(
          "Child operator's tupleDesc doesn't match that of the table to be inserted");
    }
  }

  /**
   * returns tuple desc of this operator which is a single int field with number of inserts
   *
   * @return
   */
  public TupleDesc getTupleDesc() {
    return this.td;
  }

  public void open() throws DbException, TransactionAbortedException {
    super.open();
    childOperator.open();
    this.executed = false;
  }

  public void close() {
    childOperator.close();
    super.close();
  }

  public void rewind() throws DbException, TransactionAbortedException {
    close();
    open();
  }

  /**
   * Inserts tuples read from child into the tableId specified by the constructor. It returns a one
   * field tuple containing the number of inserted records. Inserts should be passed through
   * BufferPool. An instances of BufferPool is available via Database.getBufferPool(). Note that
   * insert DOES NOT need check to see if a particular tuple is a duplicate before inserting it.
   *
   * @return A 1-field tuple containing the number of inserted records, or null if called more than
   *     once.
   * @see Database#getBufferPool
   * @see BufferPool#insertTuple
   */
  protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    if (executed) {
      return null;
    }
    executed = true;
    int insertCount = 0;
    while (childOperator.hasNext()) {
      Tuple tupleToInsert = childOperator.next();
      try {
        Database.getBufferPool().insertTuple(this.tid, this.tableId, tupleToInsert);
        insertCount++;
      } catch (IOException e) {
        throw new DbException("Heap file containing table not found");
      }
    }
    Tuple inserted = new Tuple(this.td);
    inserted.setField(0, new IntField(insertCount));
    return inserted;
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
