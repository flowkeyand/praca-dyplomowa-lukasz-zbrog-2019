package basic_statistic.basic_statistic_part_2.secondJob;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.ConstantsUtil;

import java.io.IOException;

public class Basic2MapSecond extends Mapper<LongWritable, Text, NullWritable, Basic2JobSecondTo> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splitValue = value.toString().split(ConstantsUtil.HASH_SPLITTER);
        String userId = splitValue[0];
        String countReviews = splitValue[1].replaceAll("\\s+", "");

        context.write(NullWritable.get(), new Basic2JobSecondTo(userId, countReviews));
    }
}
