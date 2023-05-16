import java.io.*;
import java.util.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class UBERStudent20201003
{
	public static class UBERMapper extends Mapper<Object,Text, Text,Text>
	{
	
		private Text word = new Text();
		private Text word2 = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String s = value.toString();
			String[] token = s.split(",");
			String day="";
                        String input = token[1];
			try{
			DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
			Date date=dateFormat.parse(input);

			Calendar c = Calendar.getInstance();
			c.setTime(date);
			int a = c.get(Calendar.DAY_OF_WEEK);
			
			switch(a){
				case 1:
					day = "SUN";
					break;
				case 2:
					day = "MON";
					break;
				case 3:
					day = "TUE";
					break;
				case 4:
					day = "WED";
					break;
				case 5:
					day = "THR";
					break;
				case 6:
					day = "FRI";
					break;
				case 7:
					day = "SAT";
					break;
			
			
			}

			word.set(token[0]+","+day);
			word2.set(token[3]+","+token[2]);
			context.write(word,word2);


			} catch(Exception e){
                                 System.out.println(e.getMessage());
                        }

		}
		
	}

	public static class UBERReducer extends Reducer<Text,Text, Text, Text>
	{
		private Text result = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			int sum_v=0;
			int sum_t=0;
			
			for (Text val : values)
			{
				
				StringTokenizer st = new StringTokenizer(val.toString(), ",");
				sum_t+=Integer.parseInt(st.nextToken());
				sum_v+=Integer.parseInt(st.nextToken());
			}
			
			//String t = Integer.toString(sum_t);
			//String v = Integer.toString(sum_v);	
			result.set(sum_v+","+sum_t);
			context.write(key, result);
		}
	}
	public static void main (String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2)
		{
			System.err.println("Usage: UberStudent20201003 <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "UBERStudent20201003");
		job.setJarByClass(UBERStudent20201003.class);
		job.setMapperClass(UBERMapper.class);
		job.setCombinerClass(UBERReducer.class);
		job.setReducerClass(UBERReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0:1);
	}
}

