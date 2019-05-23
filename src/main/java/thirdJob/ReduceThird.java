package thirdJob;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.ConstantsUtil;

import java.io.IOException;

public class ReduceThird extends Reducer<JobThirdTo, NullWritable, Text, Text> {
    private int counter = 0;

    @Override
    public void reduce(JobThirdTo key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        int numberOfRows = Integer.parseInt(context.getConfiguration().get(ConstantsUtil.NUMBER_OF_ROWS));

        if (counter < numberOfRows) {
            context.write(key.getMovieName(), new Text(key.getBayesEstimation() + " " + key.getNumberOfRatings()));
        }
        counter++;
    }
}
