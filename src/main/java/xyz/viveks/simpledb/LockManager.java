package xyz.viveks.simpledb;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
  // Map to store state on which page Id have which locks
  private Map<PageId, SimpleDbLock> pageLocks;

  private static final long TIMEOUT_MILLIS = 500;

  public LockManager() {
    pageLocks = new ConcurrentHashMap<>();
  }

  public boolean holdsLock(TransactionId tid, PageId pageId) {
    if (!pageLocks.containsKey(pageId)) {
      // No one has lock
      return false;
    }
    SimpleDbLock lockOnPage = pageLocks.get(pageId);
    return lockOnPage.holdsLock(tid);
  }

  public synchronized boolean acquireLock(TransactionId tid, PageId pageId, Permissions perm)
      throws InterruptedException {
    if (!pageLocks.containsKey(pageId)) {
      pageLocks.put(pageId, new SimpleDbLock(pageId));
    }

    if (perm == Permissions.READ_ONLY) {
      return pageLocks.get(pageId).readLock(tid, TIMEOUT_MILLIS);
    } else {
      return pageLocks.get(pageId).writeLock(tid, TIMEOUT_MILLIS);
    }
  }

  public void releaseLock(TransactionId tid, PageId pid) throws DbException {
    pageLocks.get(pid).releaseLock(tid);
  }
}
