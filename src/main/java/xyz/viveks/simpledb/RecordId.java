package xyz.viveks.simpledb;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.Objects;

/** A RecordId is a reference to a specific tuple on a specific page of a specific table. */
public final class RecordId implements Serializable {

  private static final long serialVersionUID = 1L;
  private PageId pageId;
  private int tupleNo;

  /**
   * Creates a new RecordId referring to the specified PageId and tuple number.
   *
   * @param pid the pageid of the page on which the tuple resides
   * @param tupleno the tuple number within the page.
   */
  public RecordId(PageId pid, int tupleno) {
    Preconditions.checkNotNull(pid, "pageId cannot be null");
    this.pageId = pid;
    this.tupleNo = tupleno;
  }

  /** @return the tuple number this RecordId references. */
  public int getTupleNumber() {
    return this.tupleNo;
  }

  /** @return the page id this RecordId references. */
  public PageId getPageId() {
    return this.pageId;
  }

  /**
   * Two RecordId objects are considered equal if they represent the same tuple.
   *
   * @return True if this and o represent the same tuple
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RecordId recordId = (RecordId) o;
    return tupleNo == recordId.tupleNo && pageId.equals(recordId.pageId);
  }

  /**
   * You should implement the hashCode() so that two equal RecordId instances (with respect to
   * equals()) have the same hashCode().
   *
   * @return An int that is the same for equal RecordId objects.
   */
  @Override
  public int hashCode() {
    return Objects.hash(pageId, tupleNo);
  }
}
