package simpledb;

import java.io.*;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.*;

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
    //private lockLevel = 0;
    public static final int DEFAULT_PAGES = 50;
    private int numPages = DEFAULT_PAGES;
    private HashMap<PageId, Page> idToPage;
    private LinkedList<PageId> q;
    
    private LockManager lm;
    private static long THRESHOLD = 199;
    
    public void setIdToPage(PageId pid, Page p) {
    	idToPage.put(pid, p);
    }

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        idToPage = new HashMap<PageId, Page>();
        q = new LinkedList<PageId>();
        lm = new LockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    public static int getNumPages() {
    	return DEFAULT_PAGES;
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
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
    	
    	// To implement blocking I followed he general outline in
    	// https://piazza.com/class/jqbhcmh6fbn3on?cid=420
    	// More info in writeup.
    	long start = System.currentTimeMillis();
    	long end;
    	boolean acquiredLock = false;
    	
    	acquiredLock = lm.grantLock(tid, pid, perm);
    	long timeOutThreshold  = 500 + (long) (Math.random()*500);
    	while(!acquiredLock) {
    		try {
    			end = System.currentTimeMillis();
    			if (end - start > timeOutThreshold) {
    				throw new TransactionAbortedException();
    			}
    			Thread.sleep(20 + (int) Math.random()*20);
    		} catch (TransactionAbortedException e) {
    			throw e;
    		} catch(Exception e) {
    			e.printStackTrace();
    		}
    		acquiredLock = lm.grantLock(tid, pid, perm);
    	}
    	
        if (idToPage.containsKey(pid)) {
        	q.remove(pid);
        	q.add(pid);
        	return idToPage.get(pid);
        }
        else {
        	int tableid = pid.getTableId();
        	try {
	        	DbFile f = Database.getCatalog().getDatabaseFile(tableid);
	        	Page p = f.readPage(pid);
	        	if (idToPage.size() < numPages) {
	        		idToPage.put(pid, p);
	        		q.add(pid);
	        	}
	        	else {
	        		evictPage();
	        		idToPage.put(pid, p);
	        		q.add(pid);
	        	}
	        	return p;
        	} catch (Exception e) {
        		throw new DbException("Could not find Page.");
        	}
        	
        }        
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
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    	
    	lm.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
    	return lm.holdsLock(tid, p);
        
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	try {
    	for (PageId pid : idToPage.keySet()) {
    		Page p = idToPage.get(pid);
    		if (p.isDirty() != null && p.isDirty() == tid) {
    			if (commit)
    				flushPage(pid);
    			else
    				idToPage.put(pid, p.getBeforeImage());
    		}
    	}
    	lm.releaseTransactionLocks(tid);
    	} catch (IOException e) {
    		e.printStackTrace();
    	}
    	
    	
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
    	HeapFile f = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> ps = f.insertTuple(tid, t);
        for (int i = 0; i<ps.size(); i++) {
        		Page p = ps.get(i);
        		p.markDirty(true, tid);
        		this.idToPage.put(p.getId(), p);
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
    	int tbid = t.getRecordId().getPageId().getTableId();
    	HeapFile f = (HeapFile) Database.getCatalog().getDatabaseFile(tbid);
		ArrayList<Page> ps = f.deleteTuple(tid, t);
	    for (int i = 0; i<ps.size(); i++) {
	    		Page p = ps.get(i);
	     		p.markDirty(true, tid);
	     		this.idToPage.put(p.getId(), p);
	    }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
    	for (Page p : idToPage.values()) {
			flushPage(p.getId());
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
    	idToPage.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
    	try {
	    	DbFile f = Database.getCatalog().getDatabaseFile(pid.getTableId());
	    	Page p = idToPage.get(pid);
	    	f.writePage(p);
			p.markDirty(false, null);
			this.idToPage.put(pid, p);
    	} catch (IOException e) {
    		throw e;
    	}
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	try {
        	for (PageId pid : idToPage.keySet()) {
        		Page p = idToPage.get(pid);
        		if (p.isDirty() != null && p.isDirty() == tid) 
        				flushPage(pid);
        	}
        	lm.releaseTransactionLocks(tid);
        	} catch (IOException e) {
        		e.printStackTrace();
        	}
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
    	PageId pid;
    	for (int i = 0; i < q.size(); i++) {
    		pid = q.get(i);
    		if(idToPage.get(pid) != null && idToPage.get(pid).isDirty() == null) {
    			q.remove(i);
    	        discardPage(pid);
    	        return;
    		}
    	}
    	throw new DbException("No Clean Page to Evict.");
        
        
        
    }
    
    
    
private class LockManager {
    	
    	private ConcurrentHashMap<PageId, Set<TransactionId>> sharedLocks;
    	private ConcurrentHashMap<PageId, TransactionId> exclusiveLocks;
    	
    	
    	public LockManager() {
    		sharedLocks = new ConcurrentHashMap<PageId, Set<TransactionId>>();
    		exclusiveLocks = new ConcurrentHashMap<PageId, TransactionId>();
    		
    	}
    	
    	
    	public synchronized void addSharer(TransactionId tid, PageId pid) {
    		if (sharedLocks.get(pid) == null)
    			sharedLocks.put(pid, new HashSet<TransactionId>());
    		sharedLocks.get(pid).add(tid);
    	}
    	
    	public synchronized void removeSharer(TransactionId tid, PageId pid) {
    		Set<TransactionId> sharers = sharedLocks.get(pid);
    		if (sharers.contains(tid)) {
    			sharers.remove(tid);
    		}
    	}
    	
    	public synchronized void addOwner(TransactionId tid, PageId pid) {
    		exclusiveLocks.put(pid, tid);
    	}
    	public synchronized void removeOwner(TransactionId tid, PageId pid) {
    		exclusiveLocks.remove(pid, tid);
    	}
   
    	public synchronized boolean grantSharedLock(TransactionId tid, PageId pid) {
    		Set<TransactionId> sharers = sharedLocks.get(pid);
    		TransactionId owner = exclusiveLocks.get(pid);
    		
    		// there is an owner which is not this transaction
    		if (owner != null && !owner.equals(tid))
    			return false;
    		
    		if (sharers != null && sharers.contains(tid))
    			return true;
    		
    		// no owner so we can safely add this tid to the sharers
    		// since we use a set<tid> we don't have to check if already share
    		if (owner == null)
    			addSharer(tid, pid);
    		
    		// else owner is this tid so we don't do anything
    		return true;
    		
    	}
    	
    	public synchronized boolean grantExclusiveLock(TransactionId tid, PageId pid) {
    		Set<TransactionId> sharers = sharedLocks.get(pid);
    		TransactionId owner = exclusiveLocks.get(pid);
    		
    		// there is an owner which is not this transaction
    		if (owner != null && !owner.equals(tid)) {
    			return false;
    		}
    		else if (owner != null && owner.equals(tid))
    			return true;
    		
    		if (sharers != null) {
    			
    			// either multiple sharers or 1 sharer which is not tid
    			// cannot upgrade
    			if (sharers.size() > 1 || sharers.size() == 1 && !sharers.contains(tid))
    				return false;
    			
    			// can upgrade
    			removeSharer(tid, pid);
    		}
    		addOwner(tid, pid);
    		return true;
    	}
    	
    	public synchronized boolean holdsLock (TransactionId tid, PageId pid) {
    		Set<TransactionId> sharers = sharedLocks.get(pid);
    		TransactionId owner = exclusiveLocks.get(pid);
    		
    		if (owner != null && owner.equals(tid))
    			return true;
    		
    		if (sharers != null && sharers.contains(tid))
    			return true;
    		return false;
    	}
    	
    	public synchronized void releaseLock (TransactionId tid, PageId pid) {
    		Set<TransactionId> sharers = sharedLocks.get(pid);
    		TransactionId owner = exclusiveLocks.get(pid);
    		
    		if (owner != null && owner.equals(tid)) {
    			removeOwner(tid, pid);
    		}
    		
    		if (sharers != null && sharers.contains(tid)) {
    			sharers.remove(tid);
    		}
    		return;
    	}
    	
    	public synchronized void releaseTransactionLocks(TransactionId tid) {
    		for (PageId pid : sharedLocks.keySet()) {
    			releaseLock(tid, pid);
    		}
    		for (PageId pid : exclusiveLocks.keySet()) {
    			releaseLock(tid, pid);
    		}
    	}
    	
    	public synchronized boolean grantLock(TransactionId tid, PageId pid, Permissions perm) {
    		boolean acquiredLock = false;
    		if (perm.equals(Permissions.READ_ONLY))
    			acquiredLock = grantSharedLock(tid, pid);
    		else if (perm.equals(Permissions.READ_WRITE))
    			acquiredLock = grantExclusiveLock(tid, pid);
    		return acquiredLock;
    		
    	}
    	
    	
    	
    }

}