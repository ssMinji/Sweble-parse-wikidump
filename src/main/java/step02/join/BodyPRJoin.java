package step02.join;

import java.io.IOException;

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

import com.google.gson.Gson;

public class BodyPRJoin {
	public static class DataDTO{
		private String title;
		private String descrip;
		public String getTitle() {
			return title;
		}
		public void setTitle(String title) {
			this.title = title;
		}
		public String getDescrip() {
			return descrip;
		}
		public void setDescrip(String descrip) {
			this.descrip = descrip;
		}
		@Override
		public String toString() {
			return title + "\t" + descrip;
		}
	}
	
	public static class WikiDTO {
		private String title;
		private String descrip;
		private String score;
		public String getTitle() {
			return title;
		}
		public void setTitle(String title) {
			this.title = title;
		}
		public String getDescrip() {
			return descrip;
		}
		public void setDescrip(String descrip) {
			this.descrip = descrip;
		}
		public String getScore() {
			return score;
		}
		public void setScore(String score) {
			this.score = score;
		}

		

	}
	public static class DescripMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] parts = StringUtils.splitPreserveAllTokens(value.toString(), "\t");
			String id = parts[0];
			String titleAndDescrip = parts[1];
			context.write(new Text(id), new Text("body\t" + titleAndDescrip));
		}
	}

	public static class ScoreMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] parts = StringUtils.splitPreserveAllTokens(value.toString(), "\t");
			String id = parts[0];
			String score = parts[1];
			context.write(new Text(id), new Text("PRscore\t" + score));
		}
	}

	public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String score = null;
			String data = null;

			for (Text value : values) {
				String parts[] = StringUtils.splitPreserveAllTokens(value.toString(), "\t");

				if(!parts[0].equals(null)) {
					if (parts[0].equals("body")) {
						data = parts[1];
					} else if (parts[0].equals("PRscore")) {
						score = parts[1];
					}
				} else {
					return;
				}
			}
			
			if (data != null) {
				context.write(new Text(data), new Text(score));
			}
			
//			Gson gson = new Gson();
//			DataDTO jsonData = gson.fromJson(data, DataDTO.class);
//			String title = jsonData.getTitle();
//			String descrip = jsonData.getDescrip();
//			
//			WikiDTO w = new WikiDTO();
//			w.setTitle(title);
//			w.setDescrip(descrip);
//			w.setScore(score);
//			String finalData = gson.toJson(w);
			
			
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "sweble parsing result & pagerank join");
		job.setJarByClass(BodyPRJoin.class);
		job.setReducerClass(ReduceJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(5);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, DescripMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ScoreMapper.class);
		Path outputPath = new Path(args[2]);

		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
