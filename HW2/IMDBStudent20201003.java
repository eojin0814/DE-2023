import java.awt.JobAttributes;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.omg.CORBA.PRIVATE_MEMBER;
import org.omg.CORBA.PUBLIC_MEMBER;

public class IMDBStudent20201003
{
    int k=0;
    public static class Mapper1 extends Mapper <LongWritable, Text, Text, IntWritable> {
		private Text movie = new Text();
		private static IntWritable rating = new IntWritable();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] attributes = line.split("::"); 
			movie.set(attributes[1]);
			rating.set(Integer.parseInt(attributes[2]));
			context.write(movie, rating);
		}
		
	}
    
    public static class Reducer1 extends Reducer<Text, IntWritable, Text, DoubleWritable> {
    	private static DoubleWritable ratingAvg = new DoubleWritable();
    	
    	@Override
    	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    		double total = 0;
        	double count = 0;
        	double avg = 0;        	
    		for(IntWritable rating: values) {
				total += rating.get();
				count += 1;
			}
			avg = total / count;
			ratingAvg.set(avg);
			context.write(key, ratingAvg);
		}    	    			
    }
	
    public static class Mapper2 extends Mapper<LongWritable, Text, IntWritable, Text> {
    	private final static IntWritable one = new IntWritable(1);
    	
    	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
    		context.write(one, value);
    	}
    }
    
    @SuppressWarnings("rawtypes")
	static class ValueComparator implements Comparator {
    	Map<String, Double> map;
    	
    	public ValueComparator(Map<String, Double> map) {
			this.map = map;			
		}

		public int compare(Object Key1, Object Key2) {
			Double val1 = map.get(Key1);
			Double val2 = map.get(Key2);
			
			if (val2 >= val1){
				return 1;
			}
			return -1;
		}
    	
    }
    
    public static class Reducer2 extends Reducer<IntWritable, Text, Text, DoubleWritable> {
    	private static Map<String, Double> ratingMap = new HashMap<String, Double>();
    	private Text movie = new Text();
    	private static DoubleWritable ratingAvg = new DoubleWritable();
    	
    	@Override
    	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
    		for (Text val: values){
    			String[] line = val.toString().split("[\\s]+");
    			String movie = line[0];
    			Double ratingAvg = Double.parseDouble(line[1]);
    			ratingMap.put(movie, ratingAvg);
    		}    		
    		ValueComparator bvc = new ValueComparator(ratingMap);
            @SuppressWarnings("unchecked")
			TreeMap<String, Double> sorted_map = new TreeMap<String, Double>(bvc);
            sorted_map.putAll(ratingMap);
            int count = 0;
            for (Map.Entry<String, Double> ele: sorted_map.entrySet()){
            	movie.set(ele.getKey());
            	ratingAvg.set(ele.getValue());
            	context.write(movie, ratingAvg);
            	count++;
            	if (count == k){
            		break;
            	}
            }
    	}
    }
    
	public static void main( String[] args ) throws Exception
    {
	
        Configuration config = new Configuration();
        Job job1 = Job.getInstance(config, "Top10Movies");
        
        job1.setMapperClass(Mapper1.class);
        job1.setReducerClass(Reducer1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);
        job1.setJarByClass(IMDBStudent20201003.class);
        
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
	k = Integer.parseInt(new Path(args[2]));
        boolean job1Complete = job1.waitForCompletion(true);
        
        if (job1Complete){
        	Configuration config2 = new Configuration();
            Job job2 = Job.getInstance(config2, "Top10Movies");


            job2.setMapperClass(Mapper2.class);
            job2.setReducerClass(Reducer2.class);
            job2.setMapOutputKeyClass(IntWritable.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(DoubleWritable.class);
            job2.setJarByClass(IMDBStudent20201003.class);


            job2.setInputFormatClass(TextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));

            job2.waitForCompletion(true);
        }
    }
}
