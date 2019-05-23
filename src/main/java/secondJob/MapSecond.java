package secondJob;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.ConstantsUtil;

import java.io.IOException;

public class MapSecond extends Mapper<LongWritable, Text, Text, JobSecondTo> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splitValue = value.toString().split(ConstantsUtil.COMMA_SPLITTER);
        String userId = splitValue[0];
        String rating = splitValue[1];
        String movieId = splitValue[2];
        String movieName = splitValue[3];

        JobSecondTo jobSecondTo = new JobSecondTo(rating, movieId, movieName);

        context.write(new Text(userId), jobSecondTo);
    }
}
