package fourthJob;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.ConstantsUtil;

import java.io.IOException;

public class ReduceFourth extends Reducer<JobFourthTo, NullWritable, Text, Text> {

    private int counter = 0;

    @Override
    public void reduce(JobFourthTo key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        int numberOfRows = Integer.parseInt(context.getConfiguration().get(ConstantsUtil.NUMBER_OF_ROWS));

        if (counter < numberOfRows) {
            context.write(key.getMovieName1(), new Text(key.getMovieName2() + " " + key.getSimilarity().toString() + " " + key.getNumberOfRatings()));
        }

        counter++;
    }
}
