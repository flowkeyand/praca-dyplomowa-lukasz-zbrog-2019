package advanced_statistic.firstJob;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.ConstantsUtil;

import java.io.IOException;
import java.math.BigDecimal;

public class Advanced1ReduceFirst extends Reducer<Text, DoubleWritable, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        String keyWithSeparator = key + ConstantsUtil.HASH_SPLITTER;

        BigDecimal roundedMean = new BigDecimal(mean(values)).setScale(1, BigDecimal.ROUND_HALF_UP);

        context.write(new Text(keyWithSeparator), new Text(String.valueOf(roundedMean)));
    }

    private double mean(Iterable<DoubleWritable> values) {
        double count = 0;
        double sum = 0;

        for (DoubleWritable value : values) {
            sum += value.get();
            count++;
        }

        return sum / count;
    }
}
