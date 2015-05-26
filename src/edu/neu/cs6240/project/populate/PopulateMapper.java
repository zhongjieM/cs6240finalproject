package edu.neu.cs6240.project.populate;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PopulateMapper extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] parts = value.toString().split(" ");
		if (parts.length != 3)
			return;
		RowKey rowKey = new RowKey(Integer.parseInt(parts[1]), parts[0]);
		ImmutableBytesWritable hKey = new ImmutableBytesWritable(
				rowKey.buildKey());
		Put hPut = new Put(rowKey.buildKey());
		// put word into value
		hPut.add(TableConstants.COLUMN.getBytes(),
				TableConstants.QUALIFIER_WORD.getBytes(), parts[0].getBytes());
		// put inverse P into value
		hPut.add(TableConstants.COLUMN.getBytes(),
				TableConstants.QUALIFIER_INVERSE_P.getBytes(),
				parts[2].getBytes());
		context.write(hKey, hPut);
	}

}
