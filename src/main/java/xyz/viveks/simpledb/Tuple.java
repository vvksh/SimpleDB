package xyz.viveks.simpledb;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a specified schema
 * specified by a TupleDesc object and contain Field objects with the data for each field.
 */
public class Tuple implements Serializable {

  private static final long serialVersionUID = 1L;

  private TupleDesc tupleDesc;
  private RecordId recordId;
  private List<Field> fields;

  /**
   * Create a new tuple with the specified schema (type).
   *
   * @param td the schema of this tuple. It must be a valid TupleDesc instance with at least one
   *     field.
   */
  public Tuple(TupleDesc td) {
    this.tupleDesc = td;
    this.fields = Arrays.asList(new Field[tupleDesc.numFields()]);
  }

  /** @return The TupleDesc representing the schema of this tuple. */
  public TupleDesc getTupleDesc() {
    return tupleDesc;
  }

  /** @return The RecordId representing the location of this tuple on disk. May be null. */
  public RecordId getRecordId() {
    return recordId;
  }

  /**
   * Set the RecordId information for this tuple.
   *
   * @param rid the new RecordId for this tuple.
   */
  public void setRecordId(RecordId rid) {
    recordId = rid;
  }

  /**
   * Change the value of the ith field of this tuple.
   *
   * @param i index of the field to change. It must be a valid index.
   * @param f new value for the field.
   */
  public void setField(int i, Field f) {
    Preconditions.checkElementIndex(i, fields.size());
    fields.set(i, f);
  }

  /**
   * @return the value of the ith field, or null if it has not been set.
   * @param i field index to return. Must be a valid index.
   */
  public Field getField(int i) {
    Preconditions.checkElementIndex(i, fields.size());
    return fields.get(i);
  }

  /**
   * Returns the contents of this Tuple as a string. Note that to pass the system tests, the format
   * needs to be as follows:
   *
   * <p>column1\tcolumn2\tcolumn3\t...\tcolumnN
   *
   * <p>where \t is any whitespace (except a newline)
   */
  public String toString() {
    return fields.stream().map(Objects::toString).collect(Collectors.joining(" "));
  }

  /** @return An iterator which iterates over all the fields of this tuple */
  public Iterator<Field> fields() {

    return fields.iterator();
  }

  /** reset the TupleDesc of this tuple (only affecting the TupleDesc) */
  public void resetTupleDesc(TupleDesc td) {
    this.tupleDesc = td;
  }
}
