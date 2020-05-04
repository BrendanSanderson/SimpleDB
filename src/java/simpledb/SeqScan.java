package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;
    
    private TransactionId tid;
    private int tableid;
    private String tableAlias;
    private HeapDBFileIterator hdbf_iter;
    private HeapFile hf;
    private TupleDesc td;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        this.tableid = tableid;
        this.tableAlias = tableAlias;
        hf = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        this.hdbf_iter = new HeapDBFileIterator(hf, tid);
        
        TupleDesc tdWithoutAliases = hf.getTupleDesc();
        this.td = genTupleDesc(hf, tableAlias);     
    }
    
    /**
     * Helper function for generating new TupleDesc objects using
     * the table alias.
     */
    public static TupleDesc genTupleDesc(HeapFile hf, String tableAlias) {
    	TupleDesc td = hf.getTupleDesc();
        int numFields = td.numFields();
        
        Type[] typeAr = new Type[numFields];
        String[] fieldAr = new String[numFields];        
        for (int i = 0; i < numFields; i++) {
        	typeAr[i] = td.getFieldType(i);
        	fieldAr[i] = tableAlias + "." + td.getFieldName(i);
        }
        return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableid);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        this.tableid = tableid;
        this.tableAlias = tableAlias;
        
        
        hf = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        HeapDBFileIterator hdbf_iter = new HeapDBFileIterator(hf, tid);
        
        TupleDesc tdWithoutAliases = hf.getTupleDesc();
        this.td = genTupleDesc(hf, tableAlias);
        
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
    	hdbf_iter.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return hdbf_iter.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        Tuple next = hdbf_iter.next();
        if (next == null) {
        	throw new NoSuchElementException("No Next Tuple.");
        }
        else {
        	return next;
        }
    }

    public void close() {
    	hdbf_iter.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
    	hdbf_iter.rewind();
    }
}
