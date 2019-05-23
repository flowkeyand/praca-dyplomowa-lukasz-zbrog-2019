package basic_statistic.basic_statistic_part_2.firstJob;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.ConstantsUtil;

import java.io.IOException;

public class Basic2ReduceFirst extends Reducer<Text, DoubleWritable, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        String keyWithSeparator = key + ConstantsUtil.HASH_SPLITTER;

        context.write(new Text(keyWithSeparator), new Text(String.valueOf(count(values))));
    }

    private double count(Iterable<DoubleWritable> values) {
        double count = 0;

        for (DoubleWritable value : values) {
            count++;
        }

        return count;
    }
}
