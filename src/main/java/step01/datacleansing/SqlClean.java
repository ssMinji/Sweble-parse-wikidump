package step01.datacleansing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SqlClean extends Configured implements Tool {

	public static class SqlMapper extends Mapper<Object, Text, Text, Text> {

		Map<String, String> nsAndTitle = new HashMap<>();
		private Text newId = new Text();
		private Text newTitle = new Text();

		@Override
		public void setup(Context context) throws IOException {
			// BufferedReader in = new BufferedReader(new
			// FileReader(context.getCacheFiles()[0].toString()));
			File ns_file = new File("nsfile");
			FileInputStream fis = new FileInputStream(ns_file);
			BufferedReader nsdata = new BufferedReader(new InputStreamReader(fis));

			for (String line : IOUtils.readLines(nsdata)) {
				String[] split = line.split("\t");
				String ns = split[0];
				String nsname = split[1];
				nsAndTitle.put(ns, nsname);
			}
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] parts = StringUtils.splitPreserveAllTokens(value.toString(), "\t");

			String id = parts[0];
			String title = parts[1];
			String nameSpace = parts[2];
			String newTitle = null;

			String preTitle = nsAndTitle.get(nameSpace);
			
			if (preTitle.equals(" ")) {
				newTitle = title;
			} else {
				newTitle = preTitle + ":" + title;
			}
			context.write(new Text(id), new Text(newTitle));
		}
	}

	public static class ListReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(key, value);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new SqlClean(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf, "sql cleansing");
		job.setJarByClass(this.getClass());
		job.setMapperClass(SqlMapper.class);
		job.setReducerClass(ListReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(5);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.addCacheFile(new URI("/user/mentee/input/namespace.txt#nsfile"));

		return job.waitForCompletion(true) ? 0 : 1;
	}

}
