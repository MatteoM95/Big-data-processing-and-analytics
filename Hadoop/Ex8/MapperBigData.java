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
    
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		String date = key.toString();
    		String YYYY_MM = date.split("-")[0] + "-" + date.split("-")[1];
    		float income = Float.parseFloat(value.toString());
    		
    		context.write(new Text(YYYY_MM), new FloatWritable(new Float(income)));
    }
 
}
