import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;


public class Driver {

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

		//need a class to drive map-reduce job
		//configuration and what is every job

		String inputDir = args[0];
		String nGramLibOutputDir = args[1];
		String numberOfNGram = args[2];
		String threshold = args[3];//word with frequency under threshold will be discarded
		String topk_numberOfFollowingWords = args[4];

		//two jobs
		//job1 write to dbfs and job2 write to database

		//job1
		Configuration conf1 = new Configuration();
		//set to read sentence by sentence rather than line
		//use . and a separater
		conf1.set("textinputformat.record.delimiter", ".");
		//N-gram read from command line
		conf1.set("noGram", numberOfNGram);

		//template
		Job job1 = Job.getInstance();
		job1.setJobName("NGram");
		job1.setJarByClass(Driver.class);
		
		job1.setMapperClass(NGramLibraryBuilder.NGramMapper.class);
		job1.setReducerClass(NGramLibraryBuilder.NGramReducer.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		TextInputFormat.setInputPaths(job1, new Path(inputDir));
		TextOutputFormat.setOutputPath(job1, new Path(nGramLibOutputDir));
		job1.waitForCompletion(true);




		//how to connect two jobs?
		// last output is second input
		
		//2nd job
		Configuration conf2 = new Configuration();
		conf2.set("threashold", threshold);
		conf2.set("topk", topk_numberOfFollowingWords);
		
		DBConfiguration.configureDB(conf2, 
				"com.mysql.jdbc.Driver",
				"jdbc:mysql://ip_address:port/test",
				"root",
				"password");
		/*
		driverClass
		dbUrl
			ip_address: local ip or linux
			port: 3306 8809?
			test: name of database
		 */
		
		Job job2 = Job.getInstance(conf2);
		job2.setJobName("Model");
		job2.setJarByClass(Driver.class);

		/*
		JVM talk to database --> use database connector as dependency
			local : add dependency
			hdfs : dependency upload to hdfs,
				tell job Archive(dependency) To ClassPath
		 */
		job2.addArchiveToClassPath(new Path("path_to_ur_connector"));

		//different output format in the map output and reduce output
		//we need to set MapOutputKeyClass/valueClass
			//mapper:Text, Text
			//reducer:DBOutputWritable, NullWritable
		//same output(Text, IntWritable) in the job1
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(DBOutputWritable.class);
		job2.setOutputValueClass(NullWritable.class);
		
		job2.setMapperClass(LanguageModel.Map.class);
		job2.setReducerClass(LanguageModel.Reduce.class);
		
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(DBOutputFormat.class);

		//table name is output
		//column name are "starting_phrase", "following_word", "count"
		DBOutputFormat.setOutput(job2, "output", 
				new String[] {"starting_phrase", "following_word", "count"});

		TextInputFormat.setInputPaths(job2, new Path(nGramLibOutputDir));
		job2.waitForCompletion(true);
	}

}
