package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    IntWritable,         // Output key type
                    NullWritable> {// Output value type
	
    int totRecords;

	protected void setup(Context context) {
		totRecords = 0;
	}
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		totRecords++;
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new IntWritable(totRecords), NullWritable.get());
    }
}
