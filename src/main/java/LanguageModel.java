import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class LanguageModel {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		int threshold;

		@Override
		public void setup(Context context) {
			// how to get the threashold parameter from the configuration?
			Configuration config = context.getConfiguration();
			threshold = config.getInt("threshold", 10);
		}

		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			//split between n-1 and n
			//key = n-1 words
			//value = nth word + count

			//read line by line of last map-reduce job output
			//this is cool\t20
			// "\t" separate the key and value. left side is key, right side is value


			if((value == null) || (value.toString().trim()).length() == 0) {
				return;
			}
			//this is cool\t20
			String line = value.toString().trim();
			String[] word_count = line.split("\t");

			if(word_count.length < 2){
				return;
			}

			//index0 = this is cool
			//index1 = 20

			String[] words = word_count[0].split("\\s+");
			int count = Integer.parseInt(word_count[1]);

			//how to filter the n-gram lower than threashold
			if(count < threshold){
				return;
			}

			StringBuilder sb = new StringBuilder();
			for(int i = 0; i < words.length - 1; i++){
				sb.append(words[i]).append(" ");
			}

			//this is --> cool = 20

			//what is the outputkey?
			String outputkey = sb.toString().trim();

			//what is the outputvalue?
			String outputvalue = words[words.length-1] + "=" + count;
			
			//write key-value to reducer?
			context.write(new Text(outputkey), new Text(outputvalue));
		}
	}

	public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

		int topk;
		// get the n parameter from the configuration
		//this is the k of the topk
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			topk = conf.getInt("topk", 5);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			//get topk
			//write to database

			//same key and value is a list
			//this is, <girl = 50, boy = 60>

			/*
			many way to filter the value
			approach1 : iterate list and put data in a tree map
						treeMap<count, list<word>> -->
							<1000, <shit, beautiful,...>>
							<500, <sun>>
							<50, <girl, woman>>
							...
			 */
			TreeMap<Integer, List<String>> treemap = new TreeMap<Integer, List<String>>(Collections.<Integer>reverseOrder());
			for(Text value: values){
				String currentValue = value.toString().trim();
				String word = currentValue.split("=")[0];
				int count = Integer.parseInt(currentValue.split("=")[1]);

				if(treemap.containsKey(count)){
					treemap.get(count).add(word);
				}else{
					List<String> list = new ArrayList<String>();
					list.add(word);
					treemap.put(count, list);

				}
			}

			Iterator<Integer> iterator = treemap.keySet().iterator();
			for(int i = 0; iterator.hasNext() && i < topk;){
				int count = iterator.next();
				List<String> words = treemap.get(count);
				for(String currentWord : words){
					//all output must be a key-pair
					//key is a DBOutputWritable object
					//value is null
					//database and table info are set in Driver
					context.write(new DBOutputWritable(key.toString(), currentWord, count), NullWritable.get());
					//did not set condition of topk, we can also do this in the database query limit
					i++;
				}

			}

			/*
			approach 2:
				heap <--- <count, word> && define a comparator
				topk
				use maxHeap or minHeap?

				will discuss in future class
			 */

			//can you use priorityQueue to rank topN n-gram, then write out to hdfs?
		}
	}
}
