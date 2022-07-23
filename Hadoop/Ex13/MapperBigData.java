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
						NullWritable,         // Output key type - set the appropriate data type
						Text> {// Output value type - set the appropriate data type

	String top1_date_income;
	
	protected void setup(Context context) {
		top1_date_income = null;
	}


	protected void map(
					Text key,   // Input key type
					Text value,         // Input value type
					Context context) throws IOException, InterruptedException {


		String date = key.toString();
		double income = Double.parseDouble(value.toString());
		if(top1_date_income == null)
			top1_date_income = date + "_" + income;
		else{
			String[] top1 = top1_date_income.split("_");
			String topDate = top1[0];
			double topIncome = Double.parseDouble(top1[1]);
			if(income > topIncome || (income == topIncome && date.compareTo(topDate) <= 0))
				top1_date_income = date + "_" + income;
		}
		
	}




	protected void cleanup(Context context) throws IOException, InterruptedException {
		context.write(NullWritable.get(), new Text(top1_date_income));
	}

}





//class MapperBigData extends Mapper<
//                    Text, // Input key type
//                    Text,         // Input value type
//                    NullWritable,         // Output key type
//                    DateIncome> {// Output value type
//	
//	private DateIncome top1;
//	
//	// called once for each mapper (which will analyze a subset of lines, each line is analyzed by the map method)
//	protected void setup(Context context) {
//		top1 = new DateIncome();
//		top1.setIncome(Float.MIN_VALUE);
//		top1.setDate(null);
//	}
//    
//	// called as many times as the input lines in the mapper
//    protected void map(
//            Text key,   // Input key type
//            Text value,         // Input value type
//            Context context) throws IOException, InterruptedException {
//
//    		String date = new String(key.toString());
//    		float dailyIncome = Float.parseFloat(value.toString());
//    		
//    		if(dailyIncome > top1.getIncome() || (dailyIncome == top1.getIncome() && date.compareTo(top1.getDate()) < 0)){
//    			top1 = new DateIncome();
//    			top1.setDate(date);
//    			top1.setIncome(dailyIncome);
//    		}   
//    }
//    
//    protected void cleanup(Context context) throws InterruptedException, IOException{
//    	context.write(NullWritable.get(), top1);
//    }
//}
