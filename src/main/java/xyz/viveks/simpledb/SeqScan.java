package xyz.viveks.simpledb;

import java.util.Iterator;
import java.util.NoSuchElementException;
import lombok.NonNull;
import xyz.viveks.simpledb.operators.OpIterator;

/**
 * SeqScan is an implementation of a sequential scan access method that reads each tuple of a table
 * in no particular order (e.g., as they are laid out on disk).
 */
public class SeqScan implements OpIterator {

  private static final long serialVersionUID = 1L;

  private TransactionId tid;
  private int tableId;
  private String tableAlias;

  private DbFileIterator tupleIterator;

  /**
   * Creates a sequential scan over the specified table as a part of the specified transaction.
   *
   * @param tid The transaction this scan is running as a part of.
   * @param tableid the table to scan.
   * @param tableAlias the alias of this table (needed by the parser); the returned tupleDesc should
   *     have fields with name tableAlias.fieldName (note: this class is not responsible for
   *     handling a case where tableAlias or fieldName are null. It shouldn't crash if they are, but
   *     the resulting name can be null.fieldName, tableAlias.null, or null.null).
   */
  public SeqScan(@NonNull TransactionId tid, int tableid, String tableAlias) {
    this.tid = tid;
    this.tableId = tableid;
    this.tableAlias = tableAlias;
  }

  /**
   * @return return the table name of the table the operator scans. This should be the actual name
   *     of the table in the catalog of the database
   */
  public String getTableName() {
    return Database.getCatalog().getTableName(tableId);
  }

  /** @return Return the alias of the table this operator scans. */
  public String getAlias() {
    return tableAlias;
  }

  /**
   * Reset the tableid, and tableAlias of this operator.
   *
   * @param tableid the table to scan.
   * @param tableAlias the alias of this table (needed by the parser); the returned tupleDesc should
   *     have fields with name tableAlias.fieldName (note: this class is not responsible for
   *     handling a case where tableAlias or fieldName are null. It shouldn't crash if they are, but
   *     the resulting name can be null.fieldName, tableAlias.null, or null.null).
   */
  public void reset(int tableid, String tableAlias) {
    this.tableId = tableid;
    this.tableAlias = tableAlias;
  }

  public SeqScan(TransactionId tid, int tableId) {
    this(tid, tableId, Database.getCatalog().getTableName(tableId));
  }

  public void open() throws DbException, TransactionAbortedException {
    this.tupleIterator = Database.getCatalog().getDatabaseFile(tableId).iterator(tid);
    this.tupleIterator.open();
  }

  /**
   * Returns the TupleDesc with field names from the underlying HeapFile, prefixed with the
   * tableAlias string from the constructor. This prefix becomes useful when joining tables
   * containing a field(s) with the same name. The alias and name should be separated with a "."
   * character (e.g., "alias.fieldName").
   *
   * @return the TupleDesc with field names from the underlying HeapFile, prefixed with the
   *     tableAlias string from the constructor.
   */
  public TupleDesc getTupleDesc() {
    TupleDesc td = Database.getCatalog().getTupleDesc(tableId);
    Iterator<TupleDesc.TDItem> tdItemIterator = td.iterator();
    Type[] types = new Type[td.numFields()];
    String[] fieldNames = new String[td.numFields()];
    for (int i = 0; i < td.numFields(); i++) {
      // assume its safe
      TupleDesc.TDItem currentItem = tdItemIterator.next();
      types[i] = currentItem.fieldType;
      fieldNames[i] = String.format("%s.%s", tableAlias, currentItem.fieldName);
    }
    return new TupleDesc(types, fieldNames);
  }

  public boolean hasNext() throws TransactionAbortedException, DbException {
    return this.tupleIterator.hasNext();
  }

  public Tuple next() throws NoSuchElementException, TransactionAbortedException, DbException {
    return this.tupleIterator.next();
  }

  public void close() {
    this.tupleIterator.close();
  }

  public void rewind() throws DbException, NoSuchElementException, TransactionAbortedException {
    close();
    open();
  }
}
