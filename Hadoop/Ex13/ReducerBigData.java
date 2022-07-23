package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
		NullWritable,           // Input key type
		Text,    // Input value type
		Text,           // Output key type
		FloatWritable> {  // Output value type

		@Override
		
		protected void reduce(
						NullWritable key, // Input key type
						Iterable<Text> values, // Input value type
						Context context) throws IOException, InterruptedException {

            String top1_date_income = null;
            
		    for(Text val : values){
		    	String currDate = val.toString().split("_")[0];
		    	float currIncome = Float.parseFloat(val.toString().split("_")[1]);
		    	if(top1_date_income == null)
		    		top1_date_income = currDate + "_" + currIncome;
				else{
					String[] top1 = top1_date_income.split("_");
					String topDate = top1[0];
					double topIncome = Double.parseDouble(top1[1]);
					if(currIncome > topIncome || (currIncome == topIncome && currDate.compareTo(topDate) <= 0))
						top1_date_income = currDate + "_" + currIncome;
				}
		    }
		    
		    context.write(new Text(top1_date_income.split("_")[0]),new FloatWritable(Float.parseFloat(top1_date_income.split("_")[1])));
		}
}







//class ReducerBigData extends Reducer<
//                NullWritable,           // Input key type
//                DateIncome,    // Input value type
//                Text,           // Output key type
//                FloatWritable> {  // Output value type
//    
//    @Override
//    
//    protected void reduce(
//        NullWritable key, // Input key type
//        Iterable<DateIncome> values, // Input value type
//        Context context) throws IOException, InterruptedException {
//    	
//    	String currentDate;
//    	float currentIncome;
//    	DateIncome bestDateIncome = new DateIncome();
//    	bestDateIncome.setIncome(Float.MIN_VALUE);
//    	bestDateIncome.setDate(null);
//
//        for(DateIncome top1Mapper : values) {
//        	currentDate = top1Mapper.getDate();
//        	currentIncome = top1Mapper.getIncome();
//        	if(currentIncome > bestDateIncome.getIncome() || (currentIncome == bestDateIncome.getIncome() && currentDate.compareTo(bestDateIncome.getDate()) < 0)) {
//        		bestDateIncome.setDate(currentDate);
//        		bestDateIncome.setIncome(currentIncome);
//        	}
//        }
//        
//        context.write(new Text(bestDateIncome.getDate()), new FloatWritable(new Float(bestDateIncome.getIncome())));
//    }
//}