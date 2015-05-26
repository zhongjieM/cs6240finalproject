package edu.neu.cs6240.project.predict;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PredictGrouper extends WritableComparator {

	protected PredictGrouper() {
		super(MapOutputKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		MapOutputKey key1 = (MapOutputKey) a;
		MapOutputKey key2 = (MapOutputKey) b;
		return key1.groupCompare(key2);
	}

}
