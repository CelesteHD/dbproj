package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;


import java.util.concurrent.ConcurrentHashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    private final int numPages;
    private final ConcurrentHashMap<PageId, Page> pageCache;
    private final LinkedList<PageId> lruOrder;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.pageCache = new ConcurrentHashMap<>();
	this.lruOrder = new LinkedList<>();
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
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        lockManager.acquireLock(tid, pid, perm);

        if (pageCache.containsKey(pid)) {
            touch(pid);
            return pageCache.get(pid);
        }
        if (pageCache.size() >= numPages) {
            evictPage();
        }
        DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page page = file.readPage(pid);
        pageCache.put(pid, page);
        touch(pid);
        return page;
    }
    
    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    

    public void transactionComplete(TransactionId tid, boolean commit) {
        try {
            if (commit) {
                flushPages(tid); // force dirty pages to disk
            } else {
                // abort: discard dirty pages so they revert to disk state
                for (PageId pid : lockManager.getPagesHeldBy(tid)) {
                    Page page = pageCache.get(pid);
                    if (page != null && tid.equals(page.isDirty())) {
                        discardPage(pid);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        lockManager.releaseAllLocks(tid);
    }

    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        lockManager.releaseLock(tid, pid);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.holdsLock(tid, p);
    }

    private synchronized void touch(PageId pid) {
        lruOrder.remove(pid);
        lruOrder.addLast(pid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */


    public void transactionComplete(TransactionId tid) {
        //ex 4: transactionComplete to be done here
    }

   

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
    	List<Page> dirtied = file.insertTuple(tid, t);
    	for (Page p : dirtied) {
        	p.markDirty(true, tid);
        	pageCache.put(p.getId(), p);
    	}
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
	int tableId = t.getRecordId().getPageId().getTableId();
    	DbFile file = Database.getCatalog().getDatabaseFile(tableId);
    	List<Page> dirtied = file.deleteTuple(tid, t);
    	for (Page p : dirtied) {
        	p.markDirty(true, tid);
        	pageCache.put(p.getId(), p);
    	}
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pid : pageCache.keySet()) {
                flushPage(pid);
            }
        }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        pageCache.remove(pid);
        lruOrder.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {

        Page page = pageCache.get(pid);
            if (page == null) {
                return;
            }
            if (page.isDirty() != null) {
                DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
                file.writePage(page);
                page.markDirty(false, null);
            }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        //exercise 4: flushPages to be done here
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        for (PageId pid : lruOrder) {
            Page page = pageCache.get(pid);
            if (page != null && page.isDirty() == null) {
                try { flushPage(pid); } catch (IOException e) {
                    throw new DbException("flush failed");
                }
                pageCache.remove(pid);
                lruOrder.remove(pid);
                return;
            }
        }
        throw new DbException("All pages are dirty, cannot evict");
    }
	

    private final LockManager lockManager = new LockManager();

    private class LockManager {

        // guimi Ex 5: hasDeadlock() + dfs() not implemented yet in this class

        private final ConcurrentHashMap<PageId, Set<TransactionId>> sharedLocks = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<PageId, TransactionId> exclusiveLocks = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<TransactionId, Set<PageId>> waitingFor = new ConcurrentHashMap<>();

        public synchronized void acquireLock(TransactionId tid, PageId pid, Permissions perm)
                throws TransactionAbortedException {
            if (perm == Permissions.READ_ONLY) {
                acquireSharedLock(tid, pid);
            } else {
                acquireExclusiveLock(tid, pid);
            }
        }

        private synchronized void acquireSharedLock(TransactionId tid, PageId pid)
                throws TransactionAbortedException {
            while (true) {
                TransactionId excHolder = exclusiveLocks.get(pid);
                if (excHolder == null || excHolder.equals(tid)) {
                    sharedLocks.computeIfAbsent(pid, k -> ConcurrentHashMap.newKeySet()).add(tid);
                    waitingFor.remove(tid);
                    return;
                }
                // deadlock detection done for waitingFor
                waitingFor.put(tid, Collections.singleton(pid));
                if (hasDeadlock(tid)) {
                    waitingFor.remove(tid);
                    throw new TransactionAbortedException();
                }
                try { wait(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
        }

        private synchronized void acquireExclusiveLock(TransactionId tid, PageId pid)
                throws TransactionAbortedException {
            while (true) {
                TransactionId excHolder = exclusiveLocks.get(pid);
                Set<TransactionId> shrHolders = sharedLocks.getOrDefault(pid, Collections.emptySet());

                boolean canUpgrade = shrHolders.size() == 1 && shrHolders.contains(tid) && excHolder == null;
                boolean free = excHolder == null && shrHolders.isEmpty();
                boolean weHoldExc = tid.equals(excHolder);

                if (free || weHoldExc || canUpgrade) {
                    exclusiveLocks.put(pid, tid);
                    sharedLocks.getOrDefault(pid, Collections.emptySet()).remove(tid);
                    waitingFor.remove(tid);
                    return;
                }
                // deadlock detection alrdy done for waitingFor
                waitingFor.put(tid, Collections.singleton(pid));
                if (hasDeadlock(tid)) {
                    waitingFor.remove(tid);
                    throw new TransactionAbortedException();
                }
                try { wait(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
        }


        

        public synchronized void releaseLock(TransactionId tid, PageId pid) {
            exclusiveLocks.remove(pid, tid);
            Set<TransactionId> shr = sharedLocks.get(pid);
            if (shr != null) shr.remove(tid);
            notifyAll();
        }

        public synchronized void releaseAllLocks(TransactionId tid) {
            exclusiveLocks.entrySet().removeIf(e -> e.getValue().equals(tid));
            for (Set<TransactionId> holders : sharedLocks.values()) {
                holders.remove(tid);
            }
            waitingFor.remove(tid);
            notifyAll();
        }

        public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
            TransactionId exc = exclusiveLocks.get(pid);
            if (tid.equals(exc)) return true;
            Set<TransactionId> shr = sharedLocks.get(pid);
            return shr != null && shr.contains(tid);
        }

        public synchronized Set<PageId> getPagesHeldBy(TransactionId tid) {
            Set<PageId> pages = new HashSet<>();
            for (Map.Entry<PageId, TransactionId> e : exclusiveLocks.entrySet()) {
                if (e.getValue().equals(tid)) pages.add(e.getKey());
            }
            for (Map.Entry<PageId, Set<TransactionId>> e : sharedLocks.entrySet()) {
                if (e.getValue().contains(tid)) pages.add(e.getKey());
            }
            return pages;
        }
    }

}
