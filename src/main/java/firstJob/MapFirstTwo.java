package firstJob;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.ConstantsUtil;

import java.io.IOException;

public class MapFirstTwo extends Mapper<LongWritable, Text, LongWritable, JobFirstTo> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splitValue = value.toString().split(ConstantsUtil.HASH_SPLITTER);
        String movieId = splitValue[0];
        String movieName = splitValue[1];

        context.write(new LongWritable(Long.parseLong(movieId)), new JobFirstTo(JobFirstTo.TRANSLATION.toString(), "", movieId, "", movieName));
    }
}
