package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
                    Text, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    Text> {// Output value type
    
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            String[] zoneId_date = key.toString().split(",");
            String zoneId = zoneId_date[0];
            String date = zoneId_date[1];
            Double pm10_value = Double.parseDouble(value.toString());
            
            Double threshold = new Double(50);
            
            if(pm10_value > threshold)
	            context.write(new Text(zoneId),
	                		      new Text(date));
    }
}
