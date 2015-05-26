package edu.neu.cs6240.project.calculate;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class CalculationDriver extends Configured implements Tool {

	public static final String OUTPUT_TOTAL = "TOTAL";
	public static final String OUTPUT_RANGE = "RANGE";

	private static final String awsAccessKey = "AKIAJ4Y7RNVFRZMMMPFA";
	private static final String awsSecretAccessKey = "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";
	private static final String BUCKET_NAME = "kevin017-cs6240";
	private static final String PREFIX = "pagecounts-";

	private ArrayList<String> getFilePaths(String directory) {
		// directory format is:
		// "s3://kevin017-cs6240/[other sub paths]/"
		AWSCredentials credentials = new BasicAWSCredentials(awsAccessKey,
				awsSecretAccessKey);
		AmazonS3 s3Client = new AmazonS3Client(credentials);
		ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
		listObjectsRequest.withBucketName(BUCKET_NAME);
		listObjectsRequest.withPrefix(directory.substring(21) + PREFIX);
		ObjectListing objects;
		ArrayList<String> filePaths = new ArrayList<String>();
		do {
			objects = s3Client.listObjects(listObjectsRequest);
			for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
				filePaths.add(objectSummary.getKey());
			}
			listObjectsRequest.setMarker(objects.getNextMarker());
		} while (objects.isTruncated());
		return filePaths;
	}

	public int run(String[] args) throws Exception {
		Configuration config = getConf();
		Job job = new Job(config, "Calculation");

		Path out = new Path(args[1]);
		String s3_bucket = args[0].substring(0, 21);
		for (String filePath : getFilePaths(args[0])) {
			System.out.println(s3_bucket + filePath);
			FileInputFormat.addInputPath(job, new Path(s3_bucket + filePath));
		}

		FileOutputFormat.setOutputPath(job, out);
		MultipleOutputs.addNamedOutput(job, OUTPUT_TOTAL,
				TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, OUTPUT_RANGE,
				TextOutputFormat.class, NullWritable.class, Text.class);

		job.setJarByClass(CalculationDriver.class);
		job.setMapperClass(CalculationMapper.class);
		job.setReducerClass(CalculationReducer.class);
		job.setSortComparatorClass(CalculationComparator.class);
		job.setPartitionerClass(CalculationPartitioner.class);
		job.setGroupingComparatorClass(CalculationGrouper.class);
		job.setCombinerClass(CalculationCombiner.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(MapOutputKey.class);
		job.setMapOutputValueClass(LongWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(),
				new CalculationDriver(), args);
		System.exit(result);
	}
}
