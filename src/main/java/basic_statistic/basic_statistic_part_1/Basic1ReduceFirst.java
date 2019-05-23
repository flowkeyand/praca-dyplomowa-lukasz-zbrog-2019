package basic_statistic.basic_statistic_part_1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;

public class Basic1ReduceFirst extends Reducer<NullWritable, DoubleWritable, Text, Text> {

    private static String SUM = "sum";
    private static String COUNT = "count";

    @Override
    public void reduce(NullWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        HashMap<String, Double> resultMap = ratingsSumAndCount(values);
        double ratingsSum = resultMap.get(SUM);
        double ratingsCount = resultMap.get(COUNT);

        BigDecimal roundedMean = new BigDecimal(ratingsSum / ratingsCount).setScale(2, BigDecimal.ROUND_HALF_UP);

        context.write(new Text("---------BASIC STATISTIC---------"), new Text(""));
        context.write(new Text("MEAN: "), new Text(String.valueOf(roundedMean)));
        context.write(new Text("COUNT: "), new Text(String.valueOf(ratingsCount)));
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
