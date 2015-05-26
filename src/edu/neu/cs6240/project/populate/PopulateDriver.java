package edu.neu.cs6240.project.populate;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.mapreduce.SimpleTotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PopulateDriver extends Configured implements Tool {

	public static HTable createTable(Configuration config)
			throws MasterNotRunningException, ZooKeeperConnectionException,
			IOException {
		Configuration hConfig = HBaseConfiguration.create(config);
		HBaseAdmin admin = new HBaseAdmin(hConfig);
		byte[] tableName = TableConstants.TABLE_NAME.getBytes();

		// if table exists, disable this table first then delete it so that we
		// start from scratch
		if (admin.isTableAvailable(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}

		// create table descriptor
		HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
		HColumnDescriptor columnFamily = new HColumnDescriptor(
				TableConstants.COLUMN);
		tableDescriptor.addFamily(columnFamily);
		admin.createTable(tableDescriptor);
		admin.close();
		return new HTable(hConfig, tableName);
	}

	@Override
	public int run(String[] args) throws Exception {
		// args[0]: range file directory
		// args[1]: output path
		// It takes too long to load all flight data files.
		// Thus I am considering use Bulk loading.

		Configuration config = HBaseConfiguration.create();
		// create table first, then schedule MR job.
		HTable table = PopulateDriver.createTable(config);

		Job job = new Job(config, "Populate");

		// set input and output path

		Path out = new Path(args[1]);

		// set input and output path
		FileSystem fileSystem = FileSystem.get(config);
		for (FileStatus fs : fileSystem.listStatus(new Path(args[0]))) {
			System.out.println(fs.getPath());
			FileInputFormat.addInputPath(job, fs.getPath());
		}

		FileOutputFormat.setOutputPath(job, out);

		// set jar class
		job.setJarByClass(PopulateDriver.class);

		// set mapper and reducer
		// as we do bulk loading, we don't need to implement our own reducer
		job.setMapperClass(PopulateMapper.class);
		job.setReducerClass(PutSortReducer.class);

		// set map's output key and value type
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);

		// set partitioner class
		job.setPartitionerClass(SimpleTotalOrderPartitioner.class);

		// set input and output format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(HFileOutputFormat.class);

		// configure job
		HFileOutputFormat.configureIncrementalLoad(job, table);

		int exitCode = job.waitForCompletion(true) ? 0 : 1;
		if (exitCode != 0) {
			System.err
					.println("HBase Bulk load Page Views Statistics Data FAILED");
			return exitCode;
		}

		// do bulk loading
		LoadIncrementalHFiles loader = new LoadIncrementalHFiles(getConf());
		loader.doBulkLoad(out, table);
		return exitCode;
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new PopulateDriver(),
				args);
		System.exit(result);
	}
}
