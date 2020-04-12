package xyz.viveks.simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class represents a simple lock on a page.
 *
 * <p>Each transaction may acquire read lock - if it already has write lock - if it already has read
 * lock
 *
 * <p>may acquire write lock - if it is the only one with read lock, then release it and acquire
 * write lock - if it has already a write lock
 *
 * <p>release lock - release the most recent lock, could be write or read
 */
public class SimpleDbLock {
  PageId pageId;

  enum LOCK {
    READ,
    WRITE
  };

  private Integer readLock;
  private Integer writelock;
  private Boolean locked;
  // size of keys = num transactions holding locks
  private final Map<TransactionId, LinkedList<LOCK>> acquiredLocks;

  public SimpleDbLock(PageId pageId) {
    this.pageId = pageId;
    readLock = 0;
    writelock = 0;
    locked = false;
    acquiredLocks = new ConcurrentHashMap<>();
  }

  public boolean readLock(TransactionId tid, long timeoutInMillis) throws InterruptedException {
    synchronized (acquiredLocks) {
      while (isWriteLocked()) {
        // if transaction already holds write lock, grant read lock
        if (transactionHasWriteAccess(tid)) {
          System.out.printf(
              "Transaction %s already has write lock, granting read lock\n", tid.toString());
          setReadLock(tid);
          return true;
        }
        //  if someone else holds, wait till timeout, or someone notifies
        System.out.printf("Page %s already has write lock, waiting \n", pageId.toString());
        acquiredLocks.wait(timeoutInMillis);
        if (isWriteLocked()) {
          System.out.printf(
              "Page %s already still has write lock, timing out\n", pageId.toString());
          return false;
        }
      }

      System.out.printf("Granting read lock to Transaction %s\n", tid.toString());
      setReadLock(tid);
      return true;
    }
  }

  public boolean writeLock(TransactionId tid, long timeoutInMillis) throws InterruptedException {
    synchronized (acquiredLocks) {
      while (locked) {
        // if only this transaction has any lock on this page, give it write lock
        if (acquiredLocks.keySet().size() == 1 && acquiredLocks.containsKey(tid)) {
          System.out.printf(
              "transaction: %s already has sole lock on this page, granting write lock\n",
              tid.toString());
          setWriteLock(tid);
          return true;
        }
        acquiredLocks.wait(timeoutInMillis);
        if (locked) {
          System.out.printf("Page %s already locked, timing out\n", pageId.toString());
          return false;
        }
      }

      // check if timed out

      System.out.printf("Granting write lock to Transaction %s\n", tid.toString());
      setWriteLock(tid);
      return true;
    }
  }

  public boolean transactionHasWriteAccess(TransactionId tid) {
    if (!acquiredLocks.containsKey(tid)) {
      // transaction holds no locks
      return false;
    }
    LinkedList<LOCK> locks = acquiredLocks.get(tid);
    for (LOCK l : locks) {
      if (l.equals(LOCK.WRITE)) {
        return true;
      }
    }
    return false;
  }

  boolean isWriteLocked() {
    return writelock > 0;
  }

  synchronized void setReadLock(TransactionId tid) {
    readLock++;
    if (!acquiredLocks.containsKey(tid)) {
      acquiredLocks.put(tid, new LinkedList<>());
    }
    acquiredLocks.get(tid).addLast(LOCK.READ);
    locked = true;
  }

  synchronized void setWriteLock(TransactionId tid) {
    writelock++;
    if (!acquiredLocks.containsKey(tid)) {
      acquiredLocks.put(tid, new LinkedList<>());
    }
    acquiredLocks.get(tid).addLast(LOCK.WRITE);
    locked = true;
  }

  void releaseLock(TransactionId tid) throws DbException {
    synchronized (acquiredLocks) {
      if (!acquiredLocks.containsKey(tid)) {
        throw new DbException(
            String.format("No locks acquired by transaction: %s", tid.toString()));
      }
      LOCK releasedLock = acquiredLocks.get(tid).removeLast();
      if (acquiredLocks.get(tid).isEmpty()) {
        acquiredLocks.remove(tid);
      }
      switch (releasedLock) {
        case READ:
          readLock--;
          break;
        case WRITE:
          writelock--;
          break;
      }

      if (readLock == 0 && writelock == 0) {
        locked = false;
      }

      if (readLock < 0 || writelock < 0) {
        throw new DbException("released more locks than acquired");
      }

      acquiredLocks.notify();
    }
  }

  void releaseAllLocks(TransactionId tid) throws DbException {
    synchronized (acquiredLocks) {
      if (!acquiredLocks.containsKey(tid)) {
        throw new DbException(
                String.format("No locks acquired by transaction: %s", tid.toString()));
      }
      while (!acquiredLocks.get(tid).isEmpty()) {
        LOCK releasedLock = acquiredLocks.get(tid).removeLast();
        switch (releasedLock) {
          case READ:
            readLock--;
            break;
          case WRITE:
            writelock--;
            break;
        }
      }

      if (readLock < 0 || writelock < 0) {
        throw new DbException("released more locks than acquired");
      }

      if (readLock == 0 && writelock == 0) {
        locked = false;
      }

      acquiredLocks.remove(tid);
      acquiredLocks.notify();
    }
  }

  boolean holdsLock(TransactionId tid) {
    return acquiredLocks.containsKey(tid);
  }
}
