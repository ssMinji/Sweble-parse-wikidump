package mapreducejoin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ReduceJoin {
	public static class MetaMapper extends Mapper<Object, Text, Text, Text> {
				
				public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
					String record = value.toString();
					String[] parts = record.split(",");
					context.write(new Text(parts[1]), new Text("meta\t" + parts[0]));
				}
	}

	public static class SqlMapper extends Mapper<Object, Text, Text, Text> {
				
				public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
					String record = value.toString();
					String[] parts = record.split(",");
					context.write(new Text(parts[1]), new Text("sql\t" + parts[0]));
				}
	}

	public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {
		
				public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
					String from_id = "";
					String to_id = "";
					for (Text t : values) {
						String parts[] = t.toString().split("\t");
						if (parts[0].equals("meta")) {
							to_id = parts[1];
						} else if(parts[0].equals("sql")) {
							from_id = parts[1];
						}
					}
					context.write(key, new Text(to_id));
				}
	}

	public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	Job job = new Job(conf, "Reduce-side join");
	job.setJarByClass(ReduceJoin.class);
	job.setReducerClass(ReduceJoinReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	
	
	MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, MetaMapper.class);
	MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, SqlMapper.class);
	Path outputPath = new Path(args[2]);
	
	
	FileOutputFormat.setOutputPath(job, outputPath);
	outputPath.getFileSystem(conf).delete(outputPath);
	System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}


	
	
	
	
	
	
	
	