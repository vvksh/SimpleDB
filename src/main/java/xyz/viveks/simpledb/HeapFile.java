package xyz.viveks.simpledb;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;

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
    return this.file.getAbsoluteFile().hashCode();
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
    HeapPageId heapPageId = (HeapPageId) pid;
    int pgNo = heapPageId.getPageNumber();
    Preconditions.checkNotNull(pid, "PageId cannot be null");
    Preconditions.checkArgument(pid.getPageNumber() >= 0, "pageNo cannot be less than 0");
    try {
      RandomAccessFile raf = new RandomAccessFile(file, "r");
      raf.seek(BufferPool.getPageSize() * pgNo);
      byte[] pageBytes = new byte[BufferPool.getPageSize()];
      for (int i = 0; i < BufferPool.getPageSize(); i++) {
        pageBytes[i] = raf.readByte();
      }
      return new HeapPage(heapPageId, pageBytes);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  // see DbFile.java for javadocs
  public void writePage(Page page) throws IOException {
    // some code goes here
    // not necessary for lab1
  }

  /** Returns the number of pages in this HeapFile. */
  public int numPages() {
    return (int) (file.length() / BufferPool.getPageSize());
  }

  // see DbFile.java for javadocs
  public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
      throws DbException, IOException, TransactionAbortedException {
    // some code goes here
    return null;
    // not necessary for lab1
  }

  // see DbFile.java for javadocs
  public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t)
      throws DbException, TransactionAbortedException {
    // some code goes here
    return null;
    // not necessary for lab1
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
