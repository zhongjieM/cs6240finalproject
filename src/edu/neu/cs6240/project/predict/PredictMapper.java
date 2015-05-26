package edu.neu.cs6240.project.predict;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;

import edu.neu.cs6240.project.populate.TableConstants;

public class PredictMapper extends TableMapper<MapOutputKey, IntWritable> {

	private static final long V = 12360740l;

	public static class Tuple {
		private String views;
		private String total;

		public Tuple() {
		}

		public Tuple(String views, String total) {
			this.views = views;
			this.total = total;
		}

		public String getViews() {
			return views;
		}

		public void setViews(String views) {
			this.views = views;
		}

		public String getTotal() {
			return total;
		}

		public void setTotal(String total) {
			this.total = total;
		}
	}

	private String pageName = "";
	private String[] words = null;
	private HashMap<Integer, HashMap<String, String>> cache = null;
	private HashMap<Integer, Double> inversePRMap = null;
	private HashMap<Integer, Long> rangeTotalCache = null;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		cache = new HashMap<Integer, HashMap<String, String>>();
		pageName = context.getConfiguration().get(PredictDriver.PAGE_NAME);
		words = pageName.split("_");

		/* Load range total data to calculate P(Rk) */
		inversePRMap = new HashMap<Integer, Double>();
		rangeTotalCache = new HashMap<Integer, Long>();
		String rangeTotalFilePath = context.getConfiguration().get(
				PredictDriver.RANGE_TOTAL_FILE);
		File file = new File(rangeTotalFilePath);
		long total = 0;
		try {
			BufferedReader bufferReader = new BufferedReader(new FileReader(
					file));
			String line = "";
			while ((line = bufferReader.readLine()) != null) {
				String[] parts = line.split(" ");
				if (parts.length != 2)
					continue;
				try {
					int rangeIndex = Integer.parseInt(parts[0]);
					long rangeTotal = Long.parseLong(parts[1]);
					total += rangeTotal;
					rangeTotalCache.put(rangeIndex, rangeTotal);
				} catch (Exception ex) {
					continue;
				}
			}
			bufferReader.close();
		} catch (Exception ex) {
			System.err.println(ex.toString());
		}

		for (Entry<Integer, Long> e : rangeTotalCache.entrySet()) {
			double inversePR = total * 1.0 / e.getValue();
			inversePRMap.put(e.getKey(), inversePR);
		}
	}

	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context)
			throws IOException, InterruptedException {
		byte[] _key = key.get();
		byte[] _rangeIndex = new byte[4];
		byte[] _word = value.getValue(TableConstants.COLUMN.getBytes(),
				TableConstants.QUALIFIER_WORD.getBytes());
		byte[] _inverseP = value.getValue(TableConstants.COLUMN.getBytes(),
				TableConstants.QUALIFIER_INVERSE_P.getBytes());
		for (int i = 0; i < 4; i++)
			_rangeIndex[i] = _key[i];
		ByteBuffer bbuf = ByteBuffer.wrap(_rangeIndex);
		int rangeIndex = bbuf.getInt();
		String word = new String(_word);
		String inverseP = new String(_inverseP);
		HashMap<String, String> item = cache.get(rangeIndex);
		if (item == null)
			item = new HashMap<String, String>();
		item.put(word, inverseP);
		cache.put(rangeIndex, item);
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		for (Entry<Integer, HashMap<String, String>> e : cache.entrySet()) {
			// for each existed range index
			int rangeIndex = e.getKey();
			HashMap<String, String> item = e.getValue();

			/*
			 * For range index as rangeIndex, init its inverse P(D|Rk) as
			 * inverse P(Rk)
			 */

			if (!inversePRMap.containsKey(rangeIndex)) {
				System.out.println("ERROR should contain range index;");
				continue;
			}

			double inversePR = inversePRMap.get(rangeIndex);
			BigDecimal inverse_DP = new BigDecimal(inversePR);
			for (String word : words) {
				BigDecimal inverse_P = null;
				if (item.containsKey(word)) {
					inverse_P = new BigDecimal(item.get(word));
				} else {
					// apply laplace smooth to this word
					long laplace_smooth = V + rangeTotalCache.get(rangeIndex);
					inverse_P = new BigDecimal(String.valueOf(laplace_smooth));
				}
				inverse_DP = inverse_DP.multiply(inverse_P);
			}
			context.write(new MapOutputKey(inverse_DP.toPlainString()),
					new IntWritable(rangeIndex));
		}
	}
}
