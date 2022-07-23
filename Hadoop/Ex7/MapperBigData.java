package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
                    Text, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    Text> {// Output value type
    
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		String sentenceId = key.toString();
    		
    		String words[] = value.toString().split("\\s+");
    		
    		for(String word : words) {
    			String word_lc = word.toLowerCase();
    			if(word_lc.compareTo("and") != 0 && word_lc.compareTo("not") != 0&& word_lc.compareTo("or") != 0)
    				context.write(new Text(word_lc.toLowerCase()), new Text(sentenceId));
    		}
    }
}
