package xyz.viveks.simpledb.operators;

import com.google.common.base.Preconditions;
import java.io.IOException;
import xyz.viveks.simpledb.*;

/**
 * The delete operator. Delete reads tuples from its child operator and removes them from the table
 * they belong to.
 */
public class Delete extends Operator {

  private static final long serialVersionUID = 1L;
  private TransactionId tid;
  private OpIterator childOperator;
  private TupleDesc td;
  private boolean executed;

  /**
   * Constructor specifying the transaction that this delete belongs to as well as the child to read
   * from.
   *
   * @param tid The transaction this delete runs in
   * @param child The child operator from which to read tuples for deletion
   */
  public Delete(TransactionId tid, OpIterator child) {
    this.tid = tid;
    this.childOperator = child;
    this.td = new TupleDesc(new Type[] {Type.INT_TYPE});
  }

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
   * Deletes tuples as they are read from the child operator. Deletes are processed via the buffer
   * pool (which can be accessed via the Database.getBufferPool() method.
   *
   * @return A 1-field tuple containing the number of deleted records.
   * @see Database#getBufferPool
   * @see BufferPool#deleteTuple
   */
  protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    if (executed) return null;
    executed = true;
    int deletedCount = 0;
    while (this.childOperator.hasNext()) {
      Tuple tupleToBeDeleted = this.childOperator.next();
      try {
        Database.getBufferPool().deleteTuple(tid, tupleToBeDeleted);
        deletedCount++;
      } catch (IOException e) {
        throw new DbException("IO error");
      }
    }
    Tuple deleted = new Tuple(this.td);
    deleted.setField(0, new IntField(deletedCount));
    return deleted;
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
