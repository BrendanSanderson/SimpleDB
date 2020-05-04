package simpledb;
import java.util.HashMap;
import java.util.ArrayList;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

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
    public class Sum extends AggregatorHelper {
    	private int value; 
    	public Sum(int i) {
    		value = i;
    	}
    	public void addValue(int i) {
    		value += i;
    	}
    	public int getValue() {
    		return value;
    	}
    }
    
    // note that avg could also be a class which contains
    // as fields a Sum object and a Count object
    public class Avg extends AggregatorHelper {
    	private int value;
    	private int count;
    	
    	public Avg(int i) {
    		value = i;
    		count = 1;
    	}
    	public void addValue(int i) {
    		value+= i;
    		count++;
    	}
    	public int getValue() {
    		return value / count;
    	}
    }
    
    public class Min extends AggregatorHelper {
    	private int value;
    	public Min(int i) {
    		value = i;
    	}
    	public void addValue(int i) {
    		value = (i < value? i : value);
    	}
    	public int getValue() {
    		return value;
    	}
    }
    public class Max extends AggregatorHelper {
    	private int value;
    	public Max(int i) {
    		value = i;
    	}
    	public void addValue(int i) {
    		value = (i > value? i : value);
    	}
    	public int getValue() {
    		return value;
    	}
    }

    
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.op = what;
        aggMap = new HashMap<Field, AggregatorHelper>();
        group = gbfield != NO_GROUPING;
        
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	Field groupBy;
    	AggregatorHelper iah = null;
        if (!group) {
        	groupBy = null;
        } else {
        	groupBy = tup.getField(gbfield);
        }
        int value = ((IntField) tup.getField(afield)).getValue();
        if (!aggMap.containsKey(groupBy)) {
        	switch (op) {
	        	case MIN:
	        		iah = new Min(value);
	        		break;
	        	case MAX:
	        		iah = new Max(value);
	        		break;
	        	case COUNT:
	        		iah = new Count(value);
	        		break;
	        	case SUM:
	        		iah = new Sum(value);
	        		break;
	        	case AVG:
	        		iah = new Avg(value);
	        		break;
	        }
        	aggMap.put(groupBy, iah);
        } else {
        	aggMap.get(groupBy).addValue(value);        	
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
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
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
