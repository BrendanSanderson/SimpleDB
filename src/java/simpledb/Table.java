package simpledb;

public class Table {
	private DbFile file;
	private String name;
	private String pkeyField;
	private int tableid;
	
	public Table(DbFile file, String name, String pkeyField, int tableid) {
		this.file = file;
		this.name = name;
		this.pkeyField = pkeyField;
		this.tableid = tableid;
	}
	
	public DbFile getFile() {
		return this.file;
	}
	public String getName() {
		return this.name;
	}
	public String getPkeyField() {
		return this.pkeyField;
	}
	public int getTableId() {
		return this.tableid;
	}
}
