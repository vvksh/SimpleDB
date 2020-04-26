package xyz.viveks.simpledb;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from disk. Access methods call
 * into it to retrieve pages, and it fetches pages from the appropriate location.
 *
 * <p>The BufferPool is also responsible for locking; when a transaction fetches a page, BufferPool
 * checks that the transaction has the appropriate locks to read/write the page. @Threadsafe, all
 * fields are final
 */
public class BufferPool {
  /** Bytes per page, including header. */
  private static final int DEFAULT_PAGE_SIZE = 4096;

  private static int pageSize = DEFAULT_PAGE_SIZE;

  /**
   * Default number of pages passed to the constructor. This is used by other classes. BufferPool
   * should use the numPages argument to the constructor instead.
   */
  public static final int DEFAULT_PAGES = 50;

  private int numPages;

  Map<PageId, Page> pages;

  LockManager lockManager;

  private Map<TransactionId, Set<PageId>> transactionToPagesMap;

  /**
   * Creates a BufferPool that caches up to numPages pages.
   *
   * @param numPages maximum number of pages in this buffer pool.
   */
  public BufferPool(int numPages) {
    this.numPages = numPages;
    this.pages = new LinkedHashMap<>();
    lockManager = new LockManager();
    transactionToPagesMap = new ConcurrentHashMap<>();
  }

  public static int getPageSize() {
    return pageSize;
  }

  // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
  public static void setPageSize(int pageSize) {
    BufferPool.pageSize = pageSize;
  }

  // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
  public static void resetPageSize() {
    BufferPool.pageSize = DEFAULT_PAGE_SIZE;
  }

  /**
   * Retrieve the specified page with the associated permissions. Will acquire a lock and may block
   * if that lock is held by another transaction.
   *
   * <p>The retrieved page should be looked up in the buffer pool. If it is present, it should be
   * returned. If it is not present, it should be added to the buffer pool and returned. If there is
   * insufficient space in the buffer pool, a page should be evicted and the new page should be
   * added in its place.
   *
   * @param tid the ID of the transaction requesting the page
   * @param pid the ID of the requested page
   * @param perm the requested permissions on the page
   */
  public Page getPage(TransactionId tid, PageId pid, Permissions perm)
      throws TransactionAbortedException, DbException {

    boolean acquired = false;
    try {
      acquired = lockManager.acquireLock(tid, pid, perm);
    } catch (InterruptedException e) {
      throw new TransactionAbortedException();
    }

    if (!acquired) {
      System.out.printf(
          "Transaction id %s couldnt acquire lock on page: %s with permission %s",
          tid.toString(), pid.toString(), perm.toString());
      throw new TransactionAbortedException();
    }

    // lock was successfully acquired, add to transactionToPagesMap
    if (!transactionToPagesMap.containsKey(tid)) {
      transactionToPagesMap.put(tid, new HashSet<>());
    }
    transactionToPagesMap.get(tid).add(pid);


    if (!pages.containsKey(pid)) {
      if (pages.size() == numPages) { // if bufferpool full, evict a page
        evictPage();
      }
      // get tableId for the page
      int tableId = pid.getTableId();
      DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
      this.pages.put(pid, dbFile.readPage(pid));
    }
    return pages.get(pid);
  }

  /**
   * Releases the lock on a page. Calling this is very risky, and may result in wrong behavior.
   * Think hard about who needs to call this and why, and why they can run the risk of calling it.
   *
   * @param tid the ID of the transaction requesting the unlock
   * @param pid the ID of the page to unlock
   */
  public void releasePage(TransactionId tid, PageId pid) {
    try {
      lockManager.releaseLock(tid, pid);
    } catch (DbException ex) {
      throw new RuntimeException("releasing lock without acquiring");
    }
  }

  /**
   * Release all locks associated with a given transaction.
   *
   * @param tid the ID of the transaction requesting the unlock
   */
  public void transactionComplete(TransactionId tid) throws IOException, DbException {
    transactionComplete(tid, true);
  }

  /** Return true if the specified transaction has a lock on the specified page */
  public boolean holdsLock(TransactionId tid, PageId p) {
    return lockManager.holdsLock(tid, p);
  }

  /**
   * Commit or abort a given transaction; release all locks associated to the transaction.
   *
   * @param tid the ID of the transaction requesting the unlock
   * @param commit a flag indicating whether we should commit or abort
   */
  public void transactionComplete(TransactionId tid, boolean commit) throws IOException, DbException {
    //flush all dirty pages associated with transaction to disk
    if (!transactionToPagesMap.containsKey(tid)) {
      // No pages acquired by transaction, return
      return;
    }
    if (commit) {
      for (PageId pageId: transactionToPagesMap.get(tid)) {
//        1. If found in bufferpool and dirty, flush all dirty pages to disk, note that if not dirty, page could
//         have been evicted
        if (pages.containsKey(pageId) && pages.get(pageId).isDirty() != null) {
          flushPage(pageId);
          pages.get(pageId).markDirty(false, null);
          //update page before image, so that other transactions making changes use updated page's data as before-image
          pages.get(pageId).setBeforeImage();
          Page beforeImage = pages.get(pageId).getBeforeImage();
          System.out.println(beforeImage.getId());
        }
        // 2. release all locks
        lockManager.releaseAllLocks(tid, pageId);
      }
    } else {
      for (PageId pageId: transactionToPagesMap.get(tid)) {
        //1. If found in bufferpool and dirty, restore from disk, note that if not dirty, page could
        // have been evicted
        if (pages.containsKey(pageId) && pages.get(pageId).isDirty() != null) {
          Page pageFromDisk = Database.getCatalog().getDatabaseFile(pageId.getTableId()).readPage(pageId);
          pages.put(pageId, pageFromDisk);
        }
        // 2. release all locks
        lockManager.releaseAllLocks(tid, pageId);
      }
    }
    transactionToPagesMap.remove(tid);
  }

  /**
   * Add a tuple to the specified table on behalf of transaction tid. Will acquire a write lock on
   * the page the tuple is added to and any other pages that are updated (Lock acquisition is not
   * needed for lab2). May block if the lock(s) cannot be acquired.
   *
   * <p>Marks any pages that were dirtied by the operation as dirty by calling their markDirty bit,
   * and adds versions of any pages that have been dirtied to the cache (replacing any existing
   * versions of those pages) so that future requests see up-to-date pages.
   *
   * @param tid the transaction adding the tuple
   * @param tableId the table to add the tuple to
   * @param t the tuple to add
   */
  public void insertTuple(TransactionId tid, int tableId, Tuple t)
      throws DbException, IOException, TransactionAbortedException {
    DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
    ArrayList<Page> modifiedPages = dbFile.insertTuple(tid, t);
    for (Page modifiedPage: modifiedPages) {
      modifiedPage.markDirty(true, tid);
    }
  }

  /**
   * Remove the specified tuple from the buffer pool. Will acquire a write lock on the page the
   * tuple is removed from and any other pages that are updated. May block if the lock(s) cannot be
   * acquired.
   *
   * <p>Marks any pages that were dirtied by the operation as dirty by calling their markDirty bit,
   * and adds versions of any pages that have been dirtied to the cache (replacing any existing
   * versions of those pages) so that future requests see up-to-date pages.
   *
   * @param tid the transaction deleting the tuple.
   * @param t the tuple to delete
   */
  public void deleteTuple(TransactionId tid, Tuple t)
      throws DbException, IOException, TransactionAbortedException {
    DbFile dbFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
    ArrayList<Page> modifiedPages = dbFile.deleteTuple(tid, t);
    for (Page modifiedPage: modifiedPages) {
      modifiedPage.markDirty(true, tid);
    }
  }

  /**
   * Flush all dirty pages to disk. NB: Be careful using this routine -- it writes dirty data to
   * disk so will break xyz.viveks.simpledb if running in NO STEAL mode.
   */
  public synchronized void flushAllPages() throws IOException {
    // some code goes here
    // not necessary for lab1
    for (Map.Entry<PageId, Page> pageEntry : pages.entrySet()) {
      if (pageEntry.getValue().isDirty() != null) {
        flushPage(pageEntry.getKey());
      }
    }
  }

  /**
   * Remove the specific page id from the buffer pool. Needed by the recovery manager to ensure that
   * the buffer pool doesn't keep a rolled back page in its cache.
   *
   * <p>Also used by B+ tree files to ensure that deleted pages are removed from the cache so they
   * can be reused safely
   */
  public synchronized void discardPage(PageId pid) {
      pages.remove(pid);
  }

  /**
   * Flushes a certain page to disk
   *
   * @param pid an ID indicating the page to flush
   */
  private synchronized void flushPage(PageId pid) throws IOException {
    Page pageToFlush = pages.get(pid);
    TransactionId dirtier = pageToFlush.isDirty();
    if (dirtier != null){
      Database.getLogFile().logWrite(dirtier, pageToFlush.getBeforeImage(), pageToFlush);
      Database.getLogFile().force();
    }

    DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
    dbFile.writePage(pages.get(pid));
  }

  /** Write all pages of the specified transaction to disk. */
  public synchronized void flushPages(TransactionId tid) throws IOException {
    for (PageId pageId: transactionToPagesMap.get(tid)) {
      //1. If found in bufferpool and dirty, flush all dirty pages to disk, note that if not dirty, page could
      // have been evicted
      if (pages.containsKey(pageId) && pages.get(pageId).isDirty() != null) {
        flushPage(pageId);
        //update page before image, so that other transactions making changes use updated page's data as before-image
        pages.get(pageId).setBeforeImage();
        pages.get(pageId).markDirty(false, null);
      }
    }

  }

  /**
   * Discards a page from the buffer pool. Flushes the page to disk to ensure dirty pages are
   * updated on disk.
   *
   * <p>Eviction Policy: evict the oldest page inserted, uses LinkedHashMap to preserve insertion
   * order
   */
  private synchronized void evictPage() throws DbException {
    if (pages.keySet().size() == 0) {
      throw new DbException("No pages to evict");
    }
    PageId pageIdToBeEvicted = pages.keySet().iterator().next();
    Page pageToBeEvicted = null;
    for (PageId pageId : pages.keySet()) {
      pageToBeEvicted = pages.get(pageId);
      if (pageToBeEvicted.isDirty() != null) {
        // if dirty, skip
        continue;
      }
      pageIdToBeEvicted = pageId;
      break;
    }

    if (pageIdToBeEvicted == null) {
      throw new DbException("No pages to evict");
    }
    pages.remove(pageIdToBeEvicted);
  }
}
