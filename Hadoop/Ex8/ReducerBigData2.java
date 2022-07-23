package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData2 extends Reducer<
                Text,           // Input key type
                FloatWritable,    // Input value type
                Text,           // Output key type
                FloatWritable> {  // Output value type
    
    @Override
    
    protected void reduce(
        Text key, // Input key type
        Iterable<FloatWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

    	float avg;
    	float yearly_income = new Float(0);
    	int count = 0;
    	
    	for(FloatWritable income : values) {
    		yearly_income += income.get();
    		count += 1;
    	}
    	
    	System.out.println(count);
    	System.out.println(yearly_income);
    	avg = yearly_income / count;
    	
    	context.write(key, new FloatWritable(new Float(avg)));
    }
}
