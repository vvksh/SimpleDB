package xyz.viveks.simpledb;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/** TupleDesc describes the schema of a tuple. */
public class TupleDesc implements Serializable {

  private static final long serialVersionUID = 1L;

  private List<TDItem> tdItems;

  /**
   * Create a new TupleDesc with typeAr.length fields with fields of the specified types, with
   * associated named fields.
   *
   * @param typeAr array specifying the number of and types of fields in this TupleDesc. It must
   *     contain at least one entry.
   * @param fieldAr array specifying the names of the fields. Note that names may be null.
   */
  public TupleDesc(Type[] typeAr, String[] fieldAr) {
    Preconditions.checkArgument(
        typeAr.length > 0, "type array and field name array should" + " shouldbe greater than 0 ");
    Preconditions.checkArgument(
        typeAr.length == fieldAr.length,
        "type array and field name arrays should be equal in size");

    tdItems = new ArrayList<>();
    // create a td item for each field name and type
    for (int i = 0; i < typeAr.length; i++) {
      TDItem tdItem = new TDItem(typeAr[i], fieldAr[i]);
      tdItems.add(tdItem);
    }
  }

  /**
   * Constructor. Create a new tuple desc with typeAr.length fields with fields of the specified
   * types, with anonymous (unnamed) fields.
   *
   * @param typeAr array specifying the number of and types of fields in this TupleDesc. It must
   *     contain at least one entry.
   */
  public TupleDesc(Type[] typeAr) {
    this.tdItems =
        Arrays.stream(typeAr).map(type -> new TDItem(type, null)).collect(Collectors.toList());
  }

  private TupleDesc(List<TDItem> tditems) {
    this.tdItems = tditems;
  }

  /** @return the number of fields in this TupleDesc */
  public int numFields() {
    return tdItems.size();
  }

  /**
   * Gets the (possibly null) field name of the ith field of this TupleDesc.
   *
   * @param i index of the field name to return. It must be a valid index.
   * @return the name of the ith field
   * @throws NoSuchElementException if i is not a valid field reference.
   */
  public String getFieldName(int i) throws NoSuchElementException {
    if (i < 0 || i >= tdItems.size()) {
      throw new NoSuchElementException("index is not valid");
    }
    return tdItems.get(i).fieldName;
  }

  /**
   * Gets the type of the ith field of this TupleDesc.
   *
   * @param i The index of the field to get the type of. It must be a valid index.
   * @return the type of the ith field
   * @throws NoSuchElementException if i is not a valid field reference.
   */
  public Type getFieldType(int i) throws NoSuchElementException {
    if (i < 0 || i >= tdItems.size()) {
      throw new NoSuchElementException("index is not valid");
    }
    return tdItems.get(i).fieldType;
  }

  /**
   * Find the index of the field with a given name.
   *
   * @param name name of the field.
   * @return the index of the field that is first to have the given name.
   * @throws NoSuchElementException if no field with a matching name is found.
   */
  public int fieldNameToIndex(String name) throws NoSuchElementException {
    for (int i = 0; i < tdItems.size(); i++) {
      if ((tdItems.get(i).fieldName != null) && (tdItems.get(i).fieldName).equals(name)) {
        return i;
      }
    }
    throw new NoSuchElementException("field name not found");
  }

  /**
   * @return The size (in bytes) of tuples corresponding to this TupleDesc. Note that tuples from a
   *     given TupleDesc are of a fixed size.
   */
  public int getSize() {
    return tdItems.stream().map(tdItem -> tdItem.fieldType.getLen()).reduce(0, Integer::sum);
  }

  /**
   * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields, with the first
   * td1.numFields coming from td1 and the remaining from td2.
   *
   * @param td1 The TupleDesc with the first fields of the new TupleDesc
   * @param td2 The TupleDesc with the last fields of the TupleDesc
   * @return the new TupleDesc
   */
  public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
    List<TDItem> items = new ArrayList<>();
    items.addAll(td1.tdItems);
    items.addAll(td2.tdItems);
    return new TupleDesc(items);
  }

  /**
   * Compares the specified object with this TupleDesc for equality. Two TupleDescs are considered
   * equal if they have the same number of items and if the i-th type in this TupleDesc is equal to
   * the i-th type in o for every i.
   *
   * @param o the Object to be compared for equality with this TupleDesc.
   * @return true if the object is equal to this TupleDesc.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TupleDesc tupleDesc = (TupleDesc) o;
    return Objects.equals(tdItems, tupleDesc.tdItems);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tdItems);
  }

  /**
   * Returns a String describing this descriptor. It should be of the form
   * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although the exact format does
   * not matter.
   *
   * @return String describing this descriptor.
   */
  @Override
  public String toString() {
    return "TupleDesc{" + "tdItems=" + tdItems + '}';
  }

  /**
   * @return An iterator which iterates over all the field TDItems that are included in this
   *     TupleDesc
   */
  public Iterator<TDItem> iterator() {
    return tdItems.iterator();
  }

  /** A help class to facilitate organizing the information of each field */
  public static class TDItem implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The type of the field */
    public final Type fieldType;

    /** The name of the field */
    public final String fieldName;

    public TDItem(Type t, String n) {
      this.fieldName = n;
      this.fieldType = t;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      TDItem tdItem = (TDItem) o;
      return fieldType == tdItem.fieldType && Objects.equals(fieldName, tdItem.fieldName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(fieldType, fieldName);
    }

    public String toString() {
      return fieldName + "(" + fieldType + ")";
    }
  }
}
