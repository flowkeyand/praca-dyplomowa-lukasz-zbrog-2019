package user_with_max_ratings;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.ConstantsUtil;

import java.io.IOException;

public class MaxMapFirst extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splitValue = value.toString().split(ConstantsUtil.COMMA_SPLITTER);
        String userId = splitValue[0];
        String rating = splitValue[2];

        context.write(new Text(userId), new DoubleWritable(Double.parseDouble(rating)));
    }
}
