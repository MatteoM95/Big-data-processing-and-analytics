package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
                    Text, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    FloatWritable> {// Output value type
	
	float threshold;
	
	protected void setup(Context context) {
		threshold = Float.parseFloat(context.getConfiguration().get("maxThreshold"));
	}
    
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            float pm10 = Float.parseFloat(value.toString());
            
            if(pm10 < threshold)
            	context.write(new Text(key), new FloatWritable(new Float(pm10)));
            
    }
}
