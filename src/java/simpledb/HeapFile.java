package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

	private File f;
	private TupleDesc td;
	private int id;
	
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
        this.id = f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return id;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
    	try {
    		RandomAccessFile rf = new RandomAccessFile(f, "r");
    		int offset = (pid.getPageNumber())*BufferPool.getPageSize();
    		rf.seek(offset);
    		byte[] data = new byte[BufferPool.getPageSize()];
    		rf.read(data);
    		rf.close();
    		HeapPage hp = new HeapPage((HeapPageId) pid, data);
    		return (Page) hp;
    	} catch (IOException e){
    		return null;
    	}
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
    	try {
	    	int offset = (page.getId().getPageNumber())*BufferPool.getPageSize();
	    	RandomAccessFile wf = new RandomAccessFile(f, "rw");
	    	wf.seek(offset);
	    	wf.write(page.getPageData());
			wf.close();
    	} catch (IOException e) {
    		throw e;
    	}
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) f.length()/BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
    	for(int i = 0; i < this.numPages(); i++) {
	    	HeapPageId pid = new HeapPageId(this.getId(),i);
			HeapPage p = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
			if (p.getNumEmptySlots() != 0) {
				p.insertTuple(t);
				ArrayList<Page> ps =  new ArrayList<Page>();
				ps.add(p);
				return ps;
			}
    	}
    	HeapPageId pid = new HeapPageId(this.getId(),this.numPages());
    	HeapPage p = new HeapPage(pid, HeapPage.createEmptyPageData());
    	p.insertTuple(t);	
		ArrayList<Page> ps =  new ArrayList<Page>();
		ps.add(p);
		writePage(p);
		return ps;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
    	PageId pid = t.getRecordId().getPageId();
    	HeapPage p = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
    	if (p == null) {
    		throw new DbException("Could not open Page " + pid);
    	}
    	p.deleteTuple(t);
    	ArrayList<Page> ps =  new ArrayList<Page>();
		ps.add(p);
		return ps;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        HeapDBFileIterator it = new HeapDBFileIterator(this, tid);
        return it;
    }

}

