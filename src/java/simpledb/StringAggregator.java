package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

import simpledb.Aggregator.Op;
import simpledb.IntegerAggregator.Avg;
import simpledb.IntegerAggregator.Count;
import simpledb.IntegerAggregator.Max;
import simpledb.IntegerAggregator.Min;
import simpledb.IntegerAggregator.Sum;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op op;
    private HashMap<Field, AggregatorHelper> aggMap;
    private boolean group;
    
    public class Count extends AggregatorHelper {
    	private int value; // here this is akin to count
    	public Count(int i) {
    		value = 1;
    	}
    	public void addValue(int i) {
    		value++;
    	}
    	public int getValue() {
    		return value;
    	}
    }

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.op = what;
        aggMap = new HashMap<Field, AggregatorHelper>();
        group = gbfield != NO_GROUPING;
        
        if (!what.equals(Aggregator.Op.COUNT)) {
        	throw new IllegalArgumentException("Cannot group Strings by " + what);
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	Field groupBy;
    	AggregatorHelper iah = null;
        if (!group) {
        	groupBy = null;
        } else {
        	groupBy = tup.getField(gbfield);
        }
        if (!aggMap.containsKey(groupBy)) {
        	switch (op) {
        		case COUNT:
        			iah = new Count(1); 	   // value passed to constructor gets ignored 
	        		break;
	        }
        	aggMap.put(groupBy, iah);
        } else {
        	aggMap.get(groupBy).addValue(1);   // value passed to addValue gets ignored      	
        }
    }
    
    private TupleDesc genTupleDesc() {
    	String[] nameAr;
    	Type[] typeAr;
    
    	if (!group) {
    		typeAr = new Type[] {Type.INT_TYPE};
    		nameAr = new String[] {"aggregateValue"};
    	} else {
    		typeAr = new Type[] {gbfieldtype, Type.INT_TYPE};
    		nameAr = new String[] {"groupValue", "aggregateValue"};
    	}
    	return new TupleDesc(typeAr, nameAr);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
    	TupleDesc td = genTupleDesc();
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        
        for (Field f : aggMap.keySet()) {
        	Tuple t = new Tuple(td);
        	int value = aggMap.get(f).getValue();
        	IntField intF = new IntField(value);
        	if (!group) {
        		f = intF;
        	} else {
        		t.setField(1, intF);
        	}
        	t.setField(0, f);
        	tuples.add(t);
        }
        
        return new TupleIterator(td, tuples);
    }

}
