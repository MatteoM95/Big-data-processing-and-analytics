package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                NullWritable,    // Input value type
                Text,           // Output key type
                IntWritable> {  // Output value type
	
	int contatore;
	
	protected void setup(Context context) {
		contatore = 0;
	}
    
    @Override
    
    protected void reduce(
        Text key, // Input key type
        Iterable<NullWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

    	contatore++;
        context.write(new Text(key), new IntWritable(new Integer(contatore)));
    	
    }
}

