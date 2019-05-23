package advanced_statistic.secondJob;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Advanced1ReduceSecond extends Reducer<Advanced1JobSecondTo, DoubleWritable, Text, Text> {
    @Override
    public void reduce(Advanced1JobSecondTo key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        context.write(new Text(key.getMean().toString()), new Text(String.valueOf(count(values))));
    }

    private double count(Iterable<DoubleWritable> values) {
        double count = 0;

        for (DoubleWritable value : values) {
            count++;
        }
        return count;
    }
}
