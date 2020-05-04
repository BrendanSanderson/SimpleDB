package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TupleDesc td;
    private OpIterator child; 
    private TransactionId t;
    private boolean finished = false;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
    	this.child = child;
    	Type[] types = {Type.INT_TYPE};
    	String[] names = {"Tuples Deleted"};
    	this.td = new TupleDesc(types, names);
    	this.t = t;
    }

    public TupleDesc getTupleDesc() {
    	return this.td;
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
       child.rewind();
       super.open();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if (finished) {
    		return null;
    	}
    	int ts = 0;
        try {
	        while(child.hasNext()) {
	        	Tuple tup = child.next();
	        	Database.getBufferPool().deleteTuple(t, tup);
	        	ts++;
	        }
        } catch (IOException e) {
        	throw new DbException("Lower level IO error.");
        }
        finished = true;
        Tuple delTups = new Tuple(td);
    	delTups.setField(0, new IntField(ts));
    	return delTups;
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
