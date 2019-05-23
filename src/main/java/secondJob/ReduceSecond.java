package secondJob;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.ConstantsUtil;

import java.io.IOException;
import java.util.HashMap;

public class ReduceSecond extends Reducer<Text, DoubleWritable, Text, Text> {
    private static String SUM = "sum";
    private static String COUNT = "count";

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        String keyWithSeparator = key + ConstantsUtil.HASH_SPLITTER;

        double c = Double.parseDouble(context.getConfiguration().get(ConstantsUtil.CONFIDENCE));
        double m = Double.parseDouble(context.getConfiguration().get(ConstantsUtil.MEAN));

        HashMap<String, Double> resultMap = ratingsSumAndCount(values);
        double ratingsSum = resultMap.get(SUM);
        double ratingsCount = resultMap.get(COUNT);

        context.write(new Text(keyWithSeparator), new Text(bayesEstimation(c, m, ratingsSum, ratingsCount) + ConstantsUtil.COMMA_SPLITTER + ratingsCount));
    }

    private double bayesEstimation(double c, double m, double ratingsSum, double ratingsCount) {
        return (c * m + ratingsSum) / (c + ratingsCount);
    }

    private HashMap<String, Double> ratingsSumAndCount(Iterable<DoubleWritable> values) {
        double sum = 0;
        double count = 0;

        for (DoubleWritable value : values) {
            sum += value.get();
            count++;
        }

        HashMap<String, Double> resultMap = new HashMap<>();
        resultMap.put(SUM, sum);
        resultMap.put(COUNT, count);

        return resultMap;
    }
}
