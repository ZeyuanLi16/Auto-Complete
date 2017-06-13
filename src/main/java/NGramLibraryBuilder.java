import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;


import java.io.IOException;

public class NGramLibraryBuilder {
	public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		int noGram;
		@Override
		public void setup(Context context) {
			//how to get n-gram from command line?
			Configuration config = context.getConfiguration();
			//set a default value
			noGram = config.getInt("noGram",5);


		}

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			//read sentence by sentence
			//(n-1) times split
			//write to disk(context.write)

			/*
			key is the offset of the file
			value is data read line by line (we need to change to sentence by sentence in the driver class)
			 */

			//how to remove useless elements?


			String line = value.toString().trim().toLowerCase().replaceAll("[^a-z]"," ");

			//how to separate word by space?
			//how to build n-gram based on array of words?
			String[] words = line.split("\\s+");

			if(words.length<2){
				return;
			}

			// I love big data
			for(int i = 0; i < words.length; i++ ){
				StringBuilder sb = new StringBuilder();
				sb.append(words[i]);
				for(int j = 1; i+j < words.length && j < noGram; j++){
					sb.append(" ");
					sb.append(words[j]);
					context.write(new Text(sb.toString()), new IntWritable(1));
					/*
					why Text and IntWritale?
					serialization --> after serialization, data become more light weight, int/String is an object
						Serialization is the process of converting an object into a stream of bytes
						in order to store the object or transmit it to memory, a database, or a file.
					comparable --> from mapper to reducer, shuffle --> transport and sort
					 */
				}
			}
		}
	}

	public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			//how to sum up the total count for each n-gram?

			int sum = 0;
			for(IntWritable value: values){
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

}