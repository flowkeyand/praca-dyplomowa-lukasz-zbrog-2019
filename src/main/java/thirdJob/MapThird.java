package thirdJob;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.ConstantsUtil;

import java.io.IOException;

public class MapThird extends Mapper<LongWritable, Text, JobThirdTo, NullWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splitValue = value.toString().split(ConstantsUtil.HASH_SPLITTER);
        String movieName = splitValue[0];

        String[] splitData = splitValue[1].split(ConstantsUtil.COMMA_SPLITTER);
        String bayesEstimation = splitData[0];
        String numberOfRatings = splitData[1];

        JobThirdTo jobThirdTo = new JobThirdTo(movieName, bayesEstimation, numberOfRatings);

        context.write(jobThirdTo, NullWritable.get());
    }
}
