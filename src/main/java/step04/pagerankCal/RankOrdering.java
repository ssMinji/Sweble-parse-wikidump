package step04.pagerankCal;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RankOrdering extends Mapper<LongWritable, Text, FloatWritable, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] pageAndRank = StringUtils.splitPreserveAllTokens(value.toString(), "\t");

		float parseFloat = Float.parseFloat(pageAndRank[1]);

		Text page = new Text(pageAndRank[0]);
		FloatWritable rank = new FloatWritable(parseFloat);

		context.write(rank, page);
	}
}
