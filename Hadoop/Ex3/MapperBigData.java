package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
                    Text, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {


            String[] sensorId_date = key.toString().split(",");
            String sensorId = sensorId_date[0];
            Double pm10_value = Double.parseDouble(value.toString());

            
            if(pm10_value > 50) 
            	context.write(new Text(sensorId),new IntWritable(1));
    }
}