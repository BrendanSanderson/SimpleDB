package simpledb;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;
    private StatHistogram[] hs;
	private TupleDesc td;
	private HeapFile f;
	private int cost;
	private int values;
	private int[] distinctValues;
	
	static final int DEFAULT_BUCKETS = 100;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
    	this.values = 0;
    	this.cost = ioCostPerPage;
    	this.f = (HeapFile)Database.getCatalog().getDatabaseFile(tableid);
    	this.td = f.getTupleDesc();
    	Transaction t = new Transaction();
    	HeapDBFileIterator it = new HeapDBFileIterator(f, t.getId());
    	int fields = td.numFields();
    	this.distinctValues = new int[fields];
    	int intFields = 0;

    	List<Set<Integer>> dInts  = new ArrayList<Set<Integer>>();
    	List<Set<String>> dStrings  = new ArrayList<Set<String>>();
    	
    	for(int i = 0; i < fields; i++) {
    		if (this.td.getFieldType(i).equals(Type.INT_TYPE)) {
    			intFields++;
    		}
    		 Set<String> m1 = new HashSet<String>();
    		 dStrings.add(m1);
    		 Set<Integer> m2 = new HashSet<Integer>();
    		 dInts.add(m2);
    	}
    	hs = new StatHistogram[fields];
    	int[] mins = new int[intFields];
    	int[] maxes = new int[intFields];
    	for(int i = 0; i < intFields; i++) {
    		mins[i] = Integer.MAX_VALUE;
    		maxes[i] = Integer.MIN_VALUE;
    	}
    	try {
	    	it.open();
	    	//Get mins and maxes for all int fields..

			while(it.hasNext()) {
				Tuple c = it.next();
				int ifs = 0;
				for (int i = 0; i < fields; i++) {
					if (td.getFieldType(i).equals(Type.INT_TYPE)) {
						int v = ((IntField) c.getField(i)).getValue();
						if(v < mins[ifs]) 
							mins[ifs] = v;
						else if(v > maxes[ifs]) 
							maxes[ifs] = v;
						ifs++;
					}
				}
			}
			int ifs = 0;
			//Create the IntHistograms and StringHistograms.
			for(int i = 0; i < fields; i++) {
				if(this.td.getFieldType(i).equals(Type.INT_TYPE)) {
					hs[i] = new IntHistogram(DEFAULT_BUCKETS, mins[ifs], maxes[ifs]);
					ifs++;
				}
				else 
					hs[i] = new StringHistogram(DEFAULT_BUCKETS);
			}
			it.rewind();
			//Add all values to the histograms.
			while(it.hasNext()) {
				Tuple c = it.next();
				values++;
				for (int i = 0; i < fields; i++) {
					if(td.getFieldType(i).equals(Type.INT_TYPE)) {
						int v = ((IntField) c.getField(i)).getValue();
						((IntHistogram) hs[i]).addValue(v);
						Set<Integer> set = dInts.get(i);
						set.add(v);
					}
					else {
						String v = ((StringField) c.getField(i)).getValue();
						((StringHistogram) hs[i]).addValue(v);
						Set<String> set = dStrings.get(i);
						set.add(v);
					}
				}
			}
			//For EC1: Get the amount of distinct values for each field.
			for (int i = 0; i < fields; i++) {
				if(td.getFieldType(i).equals(Type.INT_TYPE)) {
					distinctValues[i] = dInts.get(i).size();
				}
				else {
					distinctValues[i] = dStrings.get(i).size();
				}
					
			}
			
			
	    } catch(Exception e) {
			System.out.println("Could not build table stats.");
		}
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return cost * f.numPages();
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int) (values * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
    	double avg = 0;
    	if (td.getFieldType(field).equals(Type.INT_TYPE)) {
			avg = ((IntHistogram)hs[field]).avgSelectivity();
		}
    	else{
    		avg = ((StringHistogram)hs[field]).avgSelectivity();
    	}
    	double half = (1-avg)/2;
    	switch(op){
		    case EQUALS:
				return avg;
			case LESS_THAN :
				return half;
			case GREATER_THAN :
				return half;
			case LESS_THAN_OR_EQ:
				return half + avg;
			case GREATER_THAN_OR_EQ:
				return half + avg;
			case NOT_EQUALS:
				return 1 - avg;
		default:
			break;
    	}
    	return avg;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
    	if (td.getFieldType(field).equals(Type.INT_TYPE)) {
    		int v = ((IntField) constant).getValue();
    		
			return ((IntHistogram)hs[field]).estimateSelectivity(op, v);
		}
    	String v = ((StringField) constant).getValue();
		return ((StringHistogram)hs[field]).estimateSelectivity(op, v);
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
    	((IntHistogram)hs[0]).addValue(2);
    	System.out.println(((IntHistogram)hs[0]).toString());
        return values;
    }
    
    
    // For EC1: Get the distinct values for a field.
    public int getDistinctValues(int field) {
    	return distinctValues[field];
    }

}
