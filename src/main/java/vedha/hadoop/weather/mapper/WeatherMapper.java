package vedha.hadoop.weather.mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WeatherMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text event = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		Configuration conf = context.getConfiguration();
		String searchString = conf.get("searchString");
//		StringTokenizer st = new StringTokenizer(line, "[\\r\\n]+");
		if(line.contains(searchString)) {
			context.write(value, new IntWritable(1));
		}
	}
}
