package mentoring.pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

public class Xmlparser {

   public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
	      private Text title = new Text();
	      private Text id = new Text();
	
	      public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	                String string = new String(value.toString());
	                String xml = string.replaceAll("^.*<page>", "<page>") + "</page>";
	                Document doc = Jsoup.parse(xml);
	                String newid = doc.select("page>id").text();
	                String newtitle = doc.select("page>title").text();
	                if (newid.length()==0 || newtitle.length()==0) {
	                	id.set("0");
	                	title.set("xmlerror");
	                	context.write(id, title);
	                } else {
		                id.set(newid);
		                title.set(newtitle);
		                context.write(id, title);
	                }
	      }
   } 
   
   public static class ListReducer extends Reducer<Text, Text, Text, Text> {

	      public void reduce(Text key, Text values, Context context)
	            throws IOException, InterruptedException {
	    	  			context.write(key,  values);
	      }
   }

   public static void main(String[] args) throws Exception {
	  Configuration conf = new Configuration();
 	  conf.set("textinputformat.record.delimiter", "</page>");
      Job job = Job.getInstance(conf, "xml parsing");
      job.setJarByClass(Xmlparser.class);
      job.setMapperClass(TokenizerMapper.class);
      job.setReducerClass(ListReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      job.setNumReduceTasks(5);
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      System.exit(job.waitForCompletion(true) ? 0 : 1);
   }
}