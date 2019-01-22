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

public class PageRank {

   public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
      private Text title = new Text();
      private Text id = new Text();

      public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                String string = new String(value.toString());
                String xml = string.replaceAll("^.*<page>", "<page>") + "</page>";
                Document doc = Jsoup.parse(xml);
                id.set(doc.select("page>id").text());
                title.set(doc.select("page>title").text());
                   context.write(id, title);
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
      Job job = Job.getInstance(conf, "word count");
      job.setJarByClass(PageRank.class);
      job.setMapperClass(TokenizerMapper.class);
      job.setCombinerClass(ListReducer.class);
      job.setReducerClass(ListReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      System.exit(job.waitForCompletion(true) ? 0 : 1);
   }
}