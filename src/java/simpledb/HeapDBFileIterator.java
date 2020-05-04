package simpledb;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapDBFileIterator extends AbstractDbFileIterator {
	
	private HeapFile hf;
	private TransactionId tid;
	private int pInd;
	BufferPool bp = Database.getBufferPool();
	private HeapPage curhp;
	private Iterator<Tuple> curit;
	
	public HeapDBFileIterator (HeapFile hf, TransactionId tid) {
		this.hf = hf;
		this.tid = tid;
	}

	@Override
	public void open() throws DbException, TransactionAbortedException {
		pInd = 0;
		HeapPageId hpid = new HeapPageId(hf.getId(),pInd);
		curhp = (HeapPage)bp.getPage(this.tid, hpid, Permissions.READ_ONLY); 
		curit = curhp.iterator();
	}

	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		this.open();
		
	}
	@Override
	public void close() {
		super.close();
        this.curit = null;
    }

	@Override
	protected Tuple readNext() throws DbException, TransactionAbortedException {
		if (curit == null) {
			return null;
		}
		if (curit.hasNext()) {
			return curit.next();
		}
		if (pInd + 1 < hf.numPages()) {
			try {
				pInd++;
				HeapPageId hpid = new HeapPageId(hf.getId(), pInd);
				curhp = (HeapPage)bp.getPage(this.tid, hpid, Permissions.READ_ONLY); 
				this.curit = curhp.iterator();
				return readNext();
			} catch (Exception e){
				throw new DbException("Failed to access the next page.");
			}
		}
		return null;
	}
}
