package edu.neu.cs6240.project.calculate;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class CalculationReducer extends
		Reducer<MapOutputKey, LongWritable, Text, Text> {

	private MultipleOutputs<Text, Text> mos = null;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		mos = new MultipleOutputs<Text, Text>(context);
	}

	@Override
	protected void reduce(MapOutputKey key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		int rangeIndex = key.getRangeIndex();
		long total = 0;
		long count = 0;
		boolean isDummy = false;
		String pre = "";
		Iterator<LongWritable> iterator = values.iterator();
		while (iterator.hasNext()) {
			LongWritable value = iterator.next();
			if (key.getWord().equals("dummy")) {
				if (!isDummy) {
					isDummy = true;
					total = 0;
				}
				total += value.get();
				continue;
			}

			// word is not "dummy".

			if (isDummy) {
				isDummy = false;
			}

			if (key.getWord().equals(pre)) {
				count += value.get();
			} else {
				if (!pre.equals("")) {
					String op = buildRangeOutput(pre, rangeIndex, count, total);
					mos.write(CalculationDriver.OUTPUT_RANGE, null,
							new Text(op));
				}
				count = value.get();
				pre = key.getWord();
			}
		}

		if (!pre.equals("")) {
			String op = buildRangeOutput(pre, rangeIndex, count, total);
			mos.write(CalculationDriver.OUTPUT_RANGE, null, new Text(op));
		}

		mos.write(CalculationDriver.OUTPUT_TOTAL, null, new Text(rangeIndex
				+ " " + String.valueOf(total)));
	}

	private String buildRangeOutput(String word, int rangeIndex, long count,
			long total) {
		BigDecimal bigCount = new BigDecimal(String.valueOf(count));
		BigDecimal bigTotal = new BigDecimal(String.valueOf(total));
		BigDecimal inverseP = bigTotal.divide(bigCount, MathContext.DECIMAL128);
		return word + " " + rangeIndex + " " + inverseP.toPlainString();
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}

}
