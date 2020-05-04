package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private TDItem[] tds;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;

        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
    	Iterable<TDItem> iterable = Arrays.asList(tds);
        return iterable.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    	if (typeAr.length == 0)
    	{
    		return;
    	}
    	TDItem[] tempTDs = new TDItem[typeAr.length];
        for (int i = 0; i < typeAr.length; i++) {
        	tempTDs[i] = new TDItem(typeAr[i], fieldAr[i]);
        }
        
        this.tds = tempTDs;
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
    	if (typeAr.length == 0)
    	{
    		return;
    	}
    	TDItem[] tempTDs = new TDItem[typeAr.length];
        for (int i = 0; i < typeAr.length; i++) {
        	tempTDs[i] = new TDItem(typeAr[i], "");
        }
        
        this.tds = tempTDs;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return this.tds.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
    	try {
    		return tds[i].fieldName;
    	} catch (NoSuchElementException e){
    		throw e;
    	}
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
    	try {
    		return tds[i].fieldType;
    	} catch (NoSuchElementException e){
    		throw e;
    	}
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
    	for(int i = 0; i < tds.length; i++) {
    		if (tds[i].fieldName.equals(name)) {
    			
    			return i;
    		}
    	}
    	throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
    	int size = 0;
        for (int i = 0; i < tds.length; i++) {
        	size += tds[i].fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
    	int numfields = td1.numFields() + td2.numFields();
    	Type[] types = new Type[numfields];
    	String[] fields = new String[numfields];
    	for(int i = 0; i < td1.numFields(); i++) {
    		types[i] = td1.getFieldType(i);
    		fields[i] = td1.getFieldName(i);
    	}
    	for(int i = 0; i < td2.numFields(); i++) {
    		int i2 = i + td1.numFields();
    		types[i2] = td2.getFieldType(i);
    		fields[i2] = td2.getFieldName(i);
    	}
    	TupleDesc td3 = new TupleDesc(types, fields);
        return td3;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {  
        // some code goes here
    	if (!(o instanceof TupleDesc)) {
    		return false;
    	}
    	TupleDesc td = (TupleDesc) o;
    	if (this.numFields() != td.numFields()) {
    		return false;
    	}
    	for(int i = 0; i < this.numFields(); i++) {
    		if (this.getFieldType(i) != td.getFieldType(i)) {
    			return false;
    		}
    	}
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
    	String s = "";
    	for(int i = 0; i < this.numFields(); i++) {
    		s = s + this.getFieldType(i).toString() + "(" + this.getFieldName(i) + "),";
    		if (i != this.numFields()-1) {
    			s += " ";
    		}
    	}
        return s;
    }
}
