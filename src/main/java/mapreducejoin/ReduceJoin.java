package mapreducejoin;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
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
			String[] parts = StringUtils.splitPreserveAllTokens(value.toString(), "\t");
			String title = parts[1];
			String id = parts[0];
			context.write(new Text(title), new Text("TOID\t" + id));
		}
	}

	public static class SqlMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] parts = StringUtils.splitPreserveAllTokens(value.toString(), "\t");
			String title = parts[1];
			String id = parts[0];
			context.write(new Text(title), new Text("FROMID\t" + id));
		}
	}

	public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			ArrayList<String> from_id = new ArrayList<String>();
			String to_id = null;
			String pagerank = "1.0	";

			for (Text value : values) {
				String parts[] = StringUtils.splitPreserveAllTokens(value.toString(), "\t");

				if (parts[0].equals("TOID")) {
					to_id = parts[1];
				} else if (parts[0].equals("FROMID")) {
					from_id.add(parts[1]);
				}
				if (to_id == null) {
					to_id = "null";
				}
			}

			boolean first = true;
			for (String from : from_id) {
				if (!first) {
					pagerank += ",";
				}
				pagerank += from;
				first = false;
			}

			if (!to_id.equals("null")) {
				context.write(new Text(to_id), new Text(pagerank));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "xml tsv join");
		job.setJarByClass(ReduceJoin.class);
		job.setReducerClass(ReduceJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(5);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MetaMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, SqlMapper.class);
		Path outputPath = new Path(args[2]);

		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
