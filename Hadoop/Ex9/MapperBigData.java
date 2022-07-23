package it.polito.bigdata.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
				LongWritable, // Input key type
				Text,         // Input value type
				Text,         // Output key type - set the appropriate data type
				IntWritable> {// Output value type - set the appropriate data type

	HashMap<String, Integer> wordCounts;

	protected void setup(Context context) {
		wordCounts = new HashMap<String, Integer>();
	}


	protected void map(
				LongWritable key,   // Input key type
				Text value,         // Input value type
				Context context) throws IOException, InterruptedException {

		String[] words = value.toString().split("\\s+");
		for(String word : words){
			String cleanedWord = word.toLowerCase();
			Integer wordCount = wordCounts.get(cleanedWord);
			if(wordCount != null)
				wordCounts.put(new String(cleanedWord), new Integer(wordCount + 1));
			else
				wordCounts.put(new String(cleanedWord), 1);
	}


}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		for(Entry<String, Integer>pair : wordCounts.entrySet())
			context.write(new Text(pair.getKey()), new IntWritable(pair.getValue()));
}

}
