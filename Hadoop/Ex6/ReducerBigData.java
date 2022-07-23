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
                Text> {  // Output value type
    
    @Override
    
    protected void reduce(
        Text key, // Input key type
        Iterable<FloatWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	
    	float max_pm10 = Float.MIN_VALUE;
    	float min_pm10 = Float.MAX_VALUE;
    	

    	for(FloatWritable pm10_value : values) {
    		if(pm10_value.get() > max_pm10)
    			max_pm10 = pm10_value.get();
    		else 
    			if(pm10_value.get() < min_pm10)
        			min_pm10 = pm10_value.get();
    	}
    	
    	context.write(new Text(key), new Text(max_pm10 + " " + min_pm10));
    }
}
