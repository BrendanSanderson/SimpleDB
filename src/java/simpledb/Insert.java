package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private int tid;
    
    private TupleDesc td;
    private OpIterator child; 
    private TransactionId t;
    private boolean finished;
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
    	tid = tableId;
    	if (!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tid))) {
    		throw new DbException("Tuple descriptor of tuples did not match tuple descriptor of table when inserting.");
    	}
    	this.child = child;
    	Type[] types = {Type.INT_TYPE};
    	String[] names = {"Tuples Inserted"};
    	td = new TupleDesc(types, names);
    	this.t = t;
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
    	child.open();
    	super.open();
    	finished = false;
    }

    public void close() {
    	child.close();
    	super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	close();
    	open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if (finished) {
    		return null;
    	}
        int ts = 0;
        try {
	        while(child.hasNext()) {
	        	Tuple tup = child.next();
	        	Database.getBufferPool().insertTuple(t, tid, tup);
	        	ts++;
	        }
        } catch (IOException e) {
        	throw new DbException("Lower level IO error.");
        }
        finished = true;
        Tuple addedTups = new Tuple(td);
    	addedTups.setField(0, new IntField(ts));
    	return addedTups;
    }

    @Override
    public OpIterator[] getChildren() {
        
        return new OpIterator[] {child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child = children[0];
    }
}
