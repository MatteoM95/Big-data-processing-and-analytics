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
                    Text,         // Output key type
                    FloatWritable> {// Output value type
    
    protected void map(
    		LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    		
    		String sensorId = value.toString().split(",")[0];
    		Float pm10_value = Float.parseFloat(value.toString().split(",")[2]);
    		
    		context.write(new Text(sensorId), new FloatWritable(pm10_value));
    }
}

