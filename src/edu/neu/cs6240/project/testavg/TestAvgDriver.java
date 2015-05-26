package edu.neu.cs6240.project.testavg;

import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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

public class TestAvgDriver extends Configured implements Tool {

	public static final String TARGET_PAGENAME = "TargetPageName";
	private static final String awsAccessKey = "AKIAJ4Y7RNVFRZMMMPFA";
	private static final String awsSecretAccessKey = "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";
	private static final String BUCKET_NAME = "kevin017-cs6240";
	private static final String PREFIX = "pagecounts-";

	public ArrayList<String> getFilePaths(String directory) {
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
		// args[0]: input directory;
		// args[1]: output directory;
		// args[2]: test records file
		Configuration config = getConf();
		Job job = new Job(config, "Test Average");
		System.out.println(args[2]);
		DistributedCache.addCacheFile(new URI(args[2]), job.getConfiguration());

		Path out = new Path(args[1]);

		/* Set input files. */
		String s3_bucket = args[0].substring(0, 21);
		for (String filePath : getFilePaths(args[0])) {
			System.out.println(s3_bucket + filePath);
			FileInputFormat.addInputPath(job, new Path(s3_bucket + filePath));
		}

		// FileSystem fileSystem = FileSystem.get(config);
		// for (FileStatus fs : fileSystem.listStatus(new Path(args[0]))) {
		// FileInputFormat.addInputPath(job, fs.getPath());
		// }

		/* Set output file. */
		FileOutputFormat.setOutputPath(job, out);

		/* Set job components. */
		job.setJarByClass(TestAvgDriver.class);
		job.setMapperClass(TestAvgMapper.class);
		job.setReducerClass(TestAvgReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new TestAvgDriver(),
				args);
		System.exit(result);
	}
}
