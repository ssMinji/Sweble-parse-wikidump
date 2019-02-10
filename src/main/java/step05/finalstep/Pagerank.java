package step05.finalstep;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import step04.pagerankCal.RankCalculateMapper;
import step04.pagerankCal.RankCalculateReducer;
import step04.pagerankCal.RankOrdering;


public class Pagerank extends Configured implements Tool {
	
	 private static NumberFormat nf = new DecimalFormat("00");

	    public static void main(String[] args) throws Exception {
	        System.exit(ToolRunner.run(new Configuration(), new Pagerank(), args));
	    }

	    @Override
	    public int run(String[] args) throws Exception {
	    	boolean isCompleted = true;
	    	
	        String lastResultPath = null;

	        for (int runs = 0; runs < 10; runs++) {
	            String inPath = "/user/mentee/minji/iter" + nf.format(runs);
	            lastResultPath = "/user/mentee/minji/iter" + nf.format(runs + 1);

	            isCompleted = runRankCalculation(inPath, lastResultPath);

	            if (!isCompleted) return 1;
	        }

	        isCompleted = runRankOrdering(lastResultPath, "/user/mentee/minji/result");

	        if (!isCompleted) return 1;
	        return 0;
	    }
	    
	    private boolean runRankCalculation(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
	        Configuration conf = new Configuration();

	        Job rankCalculator = Job.getInstance(conf, "rankCalculator");
	        rankCalculator.setJarByClass(Pagerank.class);

	        rankCalculator.setOutputKeyClass(Text.class);
	        rankCalculator.setOutputValueClass(Text.class);

	        FileInputFormat.setInputPaths(rankCalculator, new Path(inputPath));
	        FileOutputFormat.setOutputPath(rankCalculator, new Path(outputPath));

	        rankCalculator.setMapperClass(RankCalculateMapper.class);
	        rankCalculator.setReducerClass(RankCalculateReducer.class);

	        return rankCalculator.waitForCompletion(true);
	    }

	    private boolean runRankOrdering(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
	        Configuration conf = new Configuration();

	        Job rankOrdering = Job.getInstance(conf, "rankOrdering");
	        rankOrdering.setJarByClass(Pagerank.class);

	        rankOrdering.setOutputKeyClass(FloatWritable.class);
	        rankOrdering.setOutputValueClass(Text.class);

	        rankOrdering.setMapperClass(RankOrdering.class);

	        FileInputFormat.setInputPaths(rankOrdering, new Path(inputPath));
	        FileOutputFormat.setOutputPath(rankOrdering, new Path(outputPath));

	        rankOrdering.setInputFormatClass(TextInputFormat.class);
	        rankOrdering.setOutputFormatClass(TextOutputFormat.class);

	        return rankOrdering.waitForCompletion(true);
	    }
	    

}
