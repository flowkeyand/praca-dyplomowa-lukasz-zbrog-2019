package fourthJob;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.ConstantsUtil;

import java.io.IOException;

public class MapFourth extends Mapper<LongWritable, Text, JobFourthTo, NullWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splitValue = value.toString().split(ConstantsUtil.HASH_SPLITTER);
        String movieName1 = splitValue[0];

        String[] splitData = splitValue[1].split(ConstantsUtil.COMMA_SPLITTER);
        String movieName2 = splitData[0];
        String similarity = splitData[1];
        String numberOfRatings = splitData[2];

        JobFourthTo jobFourthTo = new JobFourthTo(movieName1, similarity, movieName2, numberOfRatings);

        context.write(jobFourthTo, NullWritable.get());
    }
}
