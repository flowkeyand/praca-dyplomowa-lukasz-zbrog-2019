package history_analysis;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class HistoryAnalysisReduce extends Reducer<HistoryAnalysisTo, DoubleWritable, Text, Text> {
    private static final String MEAN = "mean";
    private static final String COUNT = "count";

    @Override
    public void reduce(HistoryAnalysisTo key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        Map<String, Double> meanData = meanData(values);

        BigDecimal roundedMean = new BigDecimal(meanData.get(MEAN)).setScale(2, BigDecimal.ROUND_HALF_UP);

        context.write(new Text(key.getYear() + "-" + String.format("%02d", key.getMonth().get() + 1)), new Text("          " + roundedMean.toString().replace(".", ",") + "        " + meanData.get(COUNT).toString().replace(".", ",")));
    }

    private Map<String, Double> meanData(Iterable<DoubleWritable> values) {
        double count = 0;
        double sum = 0;

        for (DoubleWritable value : values) {
            sum += value.get();
            count++;
        }

        double mean = sum / count;

        Map<String, Double> returnVal = new HashMap<>();
        returnVal.put(MEAN, mean);
        returnVal.put(COUNT, count);

        return returnVal;
    }
}
