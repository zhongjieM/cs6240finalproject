package edu.neu.cs6240.project.predict;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.io.WritableComparable;

public class MapOutputKey implements WritableComparable<MapOutputKey> {

	private String p;

	public MapOutputKey() {
	}

	public MapOutputKey(String p) {
		this.p = p;
	}

	public String getP() {
		return p;
	}

	public void setP(String p) {
		this.p = p;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		p = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(p);
	}

	@Override
	public int compareTo(MapOutputKey key2) {
		BigDecimal bd1 = new BigDecimal(p);
		BigDecimal bd2 = new BigDecimal(key2.getP());
		return bd1.compareTo(bd2);
	}

	public int groupCompare(MapOutputKey key2) {
		return 0;
	}

}
