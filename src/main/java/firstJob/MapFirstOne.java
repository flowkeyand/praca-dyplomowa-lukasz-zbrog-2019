package firstJob;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.ConstantsUtil;

import java.io.IOException;

public class MapFirstOne extends Mapper<LongWritable, Text, LongWritable, JobFirstTo> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splitValue = value.toString().split(ConstantsUtil.COMMA_SPLITTER);
        String userId = splitValue[0];
        String movieId = splitValue[1];
        String rating = splitValue[2];

        context.write(new LongWritable(Long.parseLong(movieId)), new JobFirstTo(JobFirstTo.MAIN.toString(), userId, movieId, rating, ""));
    }
}
