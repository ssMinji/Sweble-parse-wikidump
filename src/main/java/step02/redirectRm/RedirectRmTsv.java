package step02.redirectRm;

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

public class RedirectRmTsv {
	public static class PagelinkMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] parts = StringUtils.splitPreserveAllTokens(value.toString(), "\t");
			String id = parts[0];
			String title = parts[1];
			context.write(new Text(title), new Text("Pagelink\t" + id));
		}
	}

	public static class ReMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] parts = StringUtils.splitPreserveAllTokens(value.toString(), "\t");
			String id = parts[0];
			String title = parts[1];
			context.write(new Text(title), new Text("Redirect\t" + id));
		}
	}

	public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			ArrayList<String> linkid = new ArrayList<String>();
			ArrayList<String> redirectid = new ArrayList<String>();

			for (Text value : values) {
				String parts[] = StringUtils.splitPreserveAllTokens(value.toString(), "\t");
				if (parts[0].equals("Pagelink")) {
					linkid.add(parts[1]);
				} else if (parts[0].equals("Redirect")) {
					redirectid.add(parts[1]);
				}
			}
			if (linkid.removeAll(redirectid)) {
				for (String link : linkid) {
					context.write(new Text(link), key);
				}
			} else {
				return;
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "redirect remove from xml");
		job.setJarByClass(RedirectRmTsv.class);
		job.setReducerClass(ReduceJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(5);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PagelinkMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ReMapper.class);
		Path outputPath = new Path(args[2]);

		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
