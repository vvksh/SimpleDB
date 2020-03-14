package xyz.viveks.simpledb;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples in no particular
 * order. Tuples are stored on pages, each of which is a fixed size, and the file is simply a
 * collection of those pages. HeapFile works closely with HeapPage. The format of HeapPages is
 * described in the HeapPage constructor.
 *
 * @see HeapPage#HeapPage
 * @author Sam Madden/ viveks
 */
public class HeapFile implements DbFile {
  private File file;
  private TupleDesc td;
  private int numPages;
  private int id;

  /**
   * Constructs a heap file backed by the specified file.
   *
   * @param f the file that stores the on-disk backing store for this heap file.
   */
  public HeapFile(File f, TupleDesc td) {
    Preconditions.checkNotNull(f);
    Preconditions.checkNotNull(td);
    this.file = f;
    this.td = td;
    this.numPages = (int) (file.length() / BufferPool.getPageSize());
    this.id = file.getAbsoluteFile().hashCode();
  }

  /**
   * Returns the File backing this HeapFile on disk.
   *
   * @return the File backing this HeapFile on disk.
   */
  public File getFile() {
    return this.file;
  }

  /**
   * Returns an ID uniquely identifying this HeapFile. Implementation note: you will need to
   * generate this tableid somewhere to ensure that each HeapFile has a "unique id," and that you
   * always return the same value for a particular HeapFile. We suggest hashing the absolute file
   * name of the file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
   *
   * @return an ID uniquely identifying this HeapFile.
   */
  public int getId() {
    return this.id;
  }

  /**
   * Returns the TupleDesc of the table stored in this DbFile.
   *
   * @return TupleDesc of this DbFile.
   */
  public TupleDesc getTupleDesc() {
    return this.td;
  }

  // see DbFile.java for javadocs
  public Page readPage(PageId pid) {
    Preconditions.checkNotNull(pid, "PageId cannot be null");
    Preconditions.checkArgument(pid.getPageNumber() >= 0, "pageNo cannot be less than 0");
    int pgNo = pid.getPageNumber();
    if (pgNo > numPages) {
      throw new NoSuchElementException("Invalid pageNo.");
    }

    try {
      if (pgNo == numPages()) { // create new page
        this.numPages++;
        return new HeapPage((HeapPageId) pid, HeapPage.createEmptyPageData());
      } else { // read existing page from disk
        int pageOffset = BufferPool.getPageSize() * pgNo;
        RandomAccessFile raf = new RandomAccessFile(file, "r");
        raf.seek(pageOffset);
        byte[] pageBytes = new byte[BufferPool.getPageSize()];
        for (int i = 0; i < BufferPool.getPageSize(); i++) {
          pageBytes[i] = raf.readByte();
        }
        return new HeapPage((HeapPageId) pid, pageBytes);
      }

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  // see DbFile.java for javadocs
  public void writePage(Page page) throws IOException {
    int pageOffset = BufferPool.getPageSize() * page.getId().getPageNumber();
    RandomAccessFile raf = new RandomAccessFile(file, "rw");
    raf.seek(pageOffset);
    raf.write(page.getPageData());
  }

  /** Returns the number of pages in this HeapFile. */
  // if the file is empty, it will return 0
  public int numPages() {
    return numPages;
  }

  // see DbFile.java for javadocs

  /**
   * responsible for adding a tuple to a heap file. To add a new tuple to a HeapFile, you will have
   * to find a page with an empty slot. If no such pages exist in the HeapFile, you need to create a
   * new page and append it to the physical file on disk. You will need to ensure that the RecordID
   * in the tuple is updated correctly.
   *
   * @param tid The transaction performing the update
   * @param t The tuple to add. This tuple should be updated to reflect that it is now stored in
   *     this file.
   * @return
   * @throws DbException
   * @throws IOException
   * @throws TransactionAbortedException
   */
  public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
      throws DbException, IOException, TransactionAbortedException {
    ArrayList<Page> modifiedPages = new ArrayList<>();
    HeapPage pageWithSpace = null;
    for (int currentPageNo = 0; currentPageNo < numPages(); currentPageNo++) {
      PageId currentPageId = new HeapPageId(this.getId(), currentPageNo);
      HeapPage currentPage =
          (HeapPage) Database.getBufferPool().getPage(tid, currentPageId, Permissions.READ_ONLY);
      if (currentPage.getNumEmptySlots() > 0) {
        pageWithSpace =
            (HeapPage) Database.getBufferPool().getPage(tid, currentPageId, Permissions.READ_WRITE);
      } else {
        Database.getBufferPool().releasePage(tid, currentPageId);
      }
    }

    if (pageWithSpace != null) {
      pageWithSpace.insertTuple(t);
      modifiedPages.add(pageWithSpace);
    } else {
      // create a new page
      HeapPageId newPageId = new HeapPageId(this.getId(), numPages());
      HeapPage newPage =
          (HeapPage) Database.getBufferPool().getPage(tid, newPageId, Permissions.READ_WRITE);
      newPage.insertTuple(t);
      modifiedPages.add(newPage);
    }
    return modifiedPages;
  }

  // see DbFile.java for javadocs

  /**
   * deletes tuples. Tuples contain RecordIDs which allow you to find the page they reside on, so
   * this should be as simple as locating the page a tuple belongs to and modifying the headers of
   * the page appropriately.
   *
   * @param tid The transaction performing the update
   * @param t The tuple to delete. This tuple should be updated to reflect that it is no longer
   *     stored on any page.
   * @return
   * @throws DbException
   * @throws TransactionAbortedException
   */
  public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t)
      throws DbException, TransactionAbortedException {
    PageId pid = t.getRecordId().getPageId();
    HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
    page.deleteTuple(t);
    return Lists.newArrayList(page);
  }

  // see DbFile.java for javadocs
  public DbFileIterator iterator(TransactionId tid) {
    return new HeapDbFileIterator(this, tid);
  }
}

class HeapDbFileIterator extends AbstractDbFileIterator {

  TransactionId tid;
  HeapFile file;

  Iterator<Tuple> tupleIterator = null;

  HeapPage currentPage;

  public HeapDbFileIterator(HeapFile file, TransactionId tid) {
    this.file = file;
    this.tid = tid;
  }

  @Override
  public void open() throws DbException, TransactionAbortedException {
    currentPage = (HeapPage) getPage(0);
    tupleIterator = currentPage.iterator();
  }

  @Override
  protected Tuple readNext() throws DbException, TransactionAbortedException {
    if (tupleIterator != null && !tupleIterator.hasNext()) {
      tupleIterator = null;
    }

    // current page done iterating, start next one
    if (tupleIterator == null && currentPage != null) {
      int currentPageNo = currentPage.pid.getPageNumber();
      if (currentPageNo == file.numPages() - 1) {
        currentPage = null;
      } else {
        currentPage = (HeapPage) getPage(currentPageNo + 1);
        tupleIterator = currentPage.iterator();
        if (!tupleIterator.hasNext()) {
          tupleIterator = null;
        }
      }
    }

    if (tupleIterator == null) {
      return null;
    }
    return tupleIterator.next();
  }

  private Page getPage(int pageNo) throws TransactionAbortedException, DbException {
    return Database.getBufferPool()
        .getPage(tid, new HeapPageId(file.getId(), pageNo), Permissions.READ_ONLY);
  }

  @Override
  public void rewind() throws DbException, TransactionAbortedException {
    close();
    open();
  }

  /** close the iterator */
  @Override
  public void close() {
    super.close();
    tupleIterator = null;
    currentPage = null;
  }
}
