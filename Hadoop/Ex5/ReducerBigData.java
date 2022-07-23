package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                FloatWritable,    // Input value type
                Text,           // Output key type
                FloatWritable> {  // Output value type
    
    @Override
    
    protected void reduce(
        Text key, // Input key type
        Iterable<FloatWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	
    	System.out.println("ciao");
    	Float avg_pm10 = new Float(0);
    	Float counter = new Float(0);
    	

    	for(FloatWritable pm10_value : values) {
    		avg_pm10 += pm10_value.get();
    		counter += 1;
    	}
    	
    	avg_pm10 = avg_pm10 / counter;
    	
    	context.write(new Text(key), new FloatWritable(avg_pm10));
    }
}
