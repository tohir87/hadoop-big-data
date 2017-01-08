package in.edureka.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

public class LogAnalyser {
	public static class Map extends Mapper<LongWritable,Text,Text,IntWritable>{

		//Defining a local variable K of type Text
    	Text k= new Text();

     	//Defining a local variable v of type Text 
    	Text v= new Text(); 
    	
		public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
			
			// Read log file line by line
			String line = value.toString();
			
			//Split each line by Tab
			StringTokenizer tokenizer = new StringTokenizer(line, "\t");

			while (tokenizer.hasMoreTokens()) {
				value.set(tokenizer.nextToken());	// gets the IP address, Its expected to be the first token
				
				tokenizer.nextToken();
				tokenizer.nextToken();
				tokenizer.nextToken();
				
				//Sending to output collector which inturn passes the same to reducer
				context.write(value, new IntWritable(1));
                
			}
	
			
		}
		
	}
	
public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    	
        @Override 
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException {

        	//Defining a local variable sum of type int

            int sum = 0; 

            /*
             * Iterates through all the values available with a key and add them together 
             * and give the final result as the key and sum of its values
             */

            for(IntWritable x: values)
            {
              sum+=x.get();
            }
            
            //Dumping the output in context object
            
            context.write(key, new IntWritable(sum)); 
        } 
 
    } 

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws Exception{

		//JobConf conf = new JobConf(WordCount.class);
		Configuration conf= new Configuration();
		
		
		Job job = new Job(conf,"loganalyser");
		
		job.setJarByClass(LogAnalyser.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		

		Path outputPath = new Path(args[1]);
			
	    //Configuring the input/output path from the filesystem into the job
	        
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			//deleting the output path automatically from hdfs so that we don't have delete it explicitly
		  outputPath.getFileSystem(conf).delete(outputPath);
			
			//exiting the job only if the flag value becomes false
		  System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
