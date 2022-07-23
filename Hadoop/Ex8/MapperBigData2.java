package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData2 extends Mapper<
			LongWritable, // Input key type
			Text,         // Input value type
			Text,         // Output key type
			FloatWritable> {// Output value type

	protected void map(
		LongWritable key,   // Input key type
		Text value,         // Input value type
		Context context) throws IOException, InterruptedException {

		String date = value.toString().split(" ")[0];
		String YYYY = date.split("-")[0];
		float income = Float.parseFloat(value.toString().split("-")[1]);
		
		context.write(new Text(YYYY), new FloatWritable(new Float(income)));
		}

}
