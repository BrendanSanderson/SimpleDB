package simpledb;


/** Unique identifier for HeapPage objects. */
public class CachePageId implements Comparable {

	private PageId pid;
	private long updated;
	
    public CachePageId(PageId pid, long updated) {
    	this.pid = pid;
    	this.updated = updated;
    }

    /** @return the table associated with this PageId */
    public PageId getPageId() {
    	return pid;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public long getLastUpdated() {
        return updated;
    }

    public void update() {
    	this.updated = System.nanoTime();
    }
    
    public int compareTo(CachePageId p2) {
    	if (p2.updated > updated) {
    		return -1;
    	}
    	if (p2.updated == updated) {
    		return 0;
    	}
    	return 1;
    }

	public int compareTo(Object o) {
		try {
		CachePageId p2 = (CachePageId) o;
		if (p2.updated > updated) {
    		return -1;
    	}
    	if (p2.updated == updated) {
    		return 0;
    	}
    	return 1;
		} catch (Exception e) {
			throw e;
		}
	}

}
