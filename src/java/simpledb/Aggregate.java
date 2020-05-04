package simpledb;

import java.util.*;

import simpledb.Aggregator.Op;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    
    private OpIterator child;
    private int afield;
    private Type afieldtype;
    private int gfield;
    private Type gfieldtype;
    private Aggregator.Op aop;
    private TupleDesc td;
    private boolean group;
    
    private Aggregator agg;
    private OpIterator aggIterator;
    
    

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        td = child.getTupleDesc();
        
        group = (gfield != Aggregator.NO_GROUPING);
        
        afieldtype = td.getFieldType(afield);
        
        if (group) {
        	gfieldtype = td.getFieldType(gfield);
        } else {
        	gfieldtype = null;
        }
        
        
        if (afieldtype == Type.INT_TYPE) {
        	agg = new IntegerAggregator(gfield, gfieldtype, afield, aop);
        } else {
        	agg = new StringAggregator(gfield, gfieldtype, afield, aop);
        }
        
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
		return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
		if (!group) {
			return null;
		} else {
			return td.getFieldName(gfield);
		}
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
		return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
		return td.getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
		return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
		child.open();
		super.open();

		while (child.hasNext()) {
    		agg.mergeTupleIntoGroup(child.next());
    	}
		aggIterator = agg.iterator();
    	aggIterator.open();
		
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if (aggIterator.hasNext()) {
    		return aggIterator.next();
    	} else {
    		return null;
    	}
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	child.rewind();
//		super.rewind();
		aggIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
    	String[] nameAr;
    	Type[] typeAr;
    	
    	String aggFieldName = nameOfAggregatorOp(aop) +  td.getFieldName(afield);
    	if (group) {
    		nameAr = new String[] {groupFieldName(), aggFieldName};
    		typeAr = new Type[] {gfieldtype, afieldtype};
    	} else {
    		nameAr = new String[] {aggFieldName};
    		typeAr = new Type[] {afieldtype};
    	}
    	return new TupleDesc(typeAr, nameAr);
    }

    public void close() {
    	child.close();
		super.close();
		aggIterator.close();
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
