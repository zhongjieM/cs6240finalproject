package edu.neu.cs6240.project.predict;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.neu.cs6240.project.populate.TableConstants;

public class PredictDriver extends Configured implements Tool {

	public static final String PAGE_NAME = "PageName";
	public static final String RANGE_TOTAL_FILE = "rangeTotalFilePath";

	private Scan buildScaner(String pageName) {
		String[] words = pageName.split("_");
		Scan scan = new Scan();
		scan.setCaching(2048);
		scan.setCacheBlocks(false);
		// set OR operator.
		FilterList filterList = new FilterList(
				FilterList.Operator.MUST_PASS_ONE);
		for (int i = 0; i < words.length; i++) {
			filterList.addFilter(new SingleColumnValueFilter(
					TableConstants.COLUMN.getBytes(), Bytes
							.toBytes(TableConstants.QUALIFIER_WORD),
					CompareOp.EQUAL, words[i].getBytes()));
		}
		scan.setFilter(filterList);
		return scan;
	}

	@Override
	public int run(String[] args) throws Exception {
		// args[0]: range total file path
		// args[1]: output path
		// args[2]: page name

		Configuration config = getConf();
		// set the page name that will be used to predict
		config.set(RANGE_TOTAL_FILE, args[0]);
		config.set(PAGE_NAME, args[2]);

		Job job = new Job(config, "Prediction");
		Path out = new Path(args[1] + args[2]);

		/* Set output directory */
		FileOutputFormat.setOutputPath(job, out);

		job.setJarByClass(PredictDriver.class);
		job.setMapperClass(PredictMapper.class);
		job.setReducerClass(PredictReducer.class);
		job.setSortComparatorClass(PredictKeyComparator.class);
		job.setGroupingComparatorClass(PredictGrouper.class);
		// let the reducer do the final compare.
		job.setNumReduceTasks(1);

		TableMapReduceUtil
				.initTableMapperJob(TableConstants.TABLE_NAME.getBytes(),
						buildScaner(args[2]), PredictMapper.class,
						MapOutputKey.class, IntWritable.class, job);

		job.setInputFormatClass(TableInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(MapOutputKey.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	private static ArrayList<String> loadTestRecords(String filePath) {
		ArrayList<String> testRecords = new ArrayList<String>();

		try {
			File file = new File(filePath);
			BufferedReader bufferedReader = new BufferedReader(new FileReader(
					file));
			String testRecord = null;
			HashSet<String> exists = new HashSet<String>();
			while ((testRecord = bufferedReader.readLine()) != null) {
				String[] parts = testRecord.split("\t");
				Random r = new Random();
				float chance = r.nextFloat();
				if (chance <= 0.04758f) {
					String page_name = parts[0].trim();
					if (!exists.contains(page_name) && !page_name.equals("")) {
						testRecords.add(page_name);
						exists.add(page_name);
					}
				}
			}
			bufferedReader.close();
		} catch (Exception ex) {
			System.err.println("Failed to load files.");
			System.out.println(ex.toString());
		}
		return testRecords;
	}

	public static void main(String[] args) throws Exception {
		// args[0]: range total file path
		// args[1]: output directory ending with '/'
		// args[2]: page names test records file
		ArrayList<String> testRecords = loadTestRecords(args[2]);
		for (String testRecord : testRecords) {
			String[] _args = new String[args.length];
			for (int i = 0; i < args.length - 1; i++)
				_args[i] = args[i];
			_args[args.length - 1] = testRecord;
			try {
				int result = ToolRunner.run(new Configuration(),
						new PredictDriver(), _args);
				if (result == 0)
					System.out.println("Prediction for " + testRecord
							+ " succeeded.");
				else
					System.out.println("Prediction for " + testRecord
							+ " FAILED!");
			} catch (Exception ex) {
				continue;
			}
		}
	}

}
