package user_with_max_ratings;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.StorageUtil;

import java.io.IOException;

public class MaxReduceFirst extends Reducer<Text, DoubleWritable, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) {
        double countedRatings = count(values);

        if (countedRatings > StorageUtil.MAX_TO.getMax()) {
            StorageUtil.MAX_TO.update(key.toString(), countedRatings);
        }
    }

    private double count(Iterable<DoubleWritable> values) {
        double count = 0;

        for (DoubleWritable value : values) {
            count++;
        }

        return count;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.getCounter("userWithMaxRatingsJob", MaxTo.USER_ID).increment(Long.parseLong(StorageUtil.MAX_TO.getUserId()));
        context.getCounter("userWithMaxRatingsJob", MaxTo.MAX).increment((long) StorageUtil.MAX_TO.getMax());

        context.write(new Text("---------AVG RATING PER USER IN YEARS---------"), new Text(""));
        context.write(new Text("------------------BASIC INFO------------------"), new Text(""));
        context.write(new Text("USER ID: " + StorageUtil.MAX_TO.getUserId()), new Text(""));
        context.write(new Text("NUMBER_OF_RATINGS: " + StorageUtil.MAX_TO.getMax()), new Text(""));
        context.write(new Text("----------------------------------------------"), new Text(""));
        context.write(new Text("------------------MAIN DATA-------------------"), new Text(""));
        context.write(new Text("YEAR_AND_MONTH    AVG_RATING  COUNT"), new Text(""));
    }
}
