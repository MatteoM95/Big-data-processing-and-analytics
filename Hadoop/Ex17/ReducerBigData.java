package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
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

        float maxTemp = Float.MIN_VALUE;
        for(FloatWritable temp : values) {
        	if(temp.get() > maxTemp)
        		maxTemp = temp.get();
        }
        
        context.write(new Text(key), new FloatWritable(new Float(maxTemp)));
    }
}
