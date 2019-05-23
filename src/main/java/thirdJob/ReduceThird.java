package thirdJob;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.ConstantsUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ReduceThird extends Reducer<Text, Text, Text, Text> {

    private static final String SCORE = "score";
    private static final String NUM_PAIRS = "numPairs";

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String[] splitLine = key.toString().trim().split(ConstantsUtil.COMMA_SPLITTER);

        String movieName1 = splitLine[1];
        String movieName2 = splitLine[3];

        if (movieName1.equals(context.getConfiguration().get(ConstantsUtil.TARGET_FILM))) {
            Map<String, Double> similarityResult = cosineSimilarity(values);

            Double numPairs = similarityResult.get(NUM_PAIRS);
            Double score = similarityResult.get(SCORE);

            Double userMinNumPairs = Double.parseDouble(context.getConfiguration().get(ConstantsUtil.MIN_NUMBER_PAIRS));
            Double userMinScore = Double.parseDouble(context.getConfiguration().get(ConstantsUtil.MIN_SCORE));

            if (numPairs >= userMinNumPairs && score >= userMinScore) {
                context.write(new Text(movieName1 + ConstantsUtil.HASH_SPLITTER), new Text(movieName2 + ConstantsUtil.COMMA_SPLITTER + score + ConstantsUtil.COMMA_SPLITTER + numPairs));
            }
        }
    }

    private Map<String, Double> cosineSimilarity(Iterable<Text> values) {
        double numPairs = 0;
        double sumXX = 0;
        double sumYY = 0;
        double sumXY = 0;

        for (Text singlePair : values) {
            String[] splitEntry = singlePair.toString().trim().split(ConstantsUtil.COMMA_SPLITTER);
            double ratingX = Double.parseDouble(splitEntry[0]);
            double ratingY = Double.parseDouble(splitEntry[1]);

            sumXX += ratingX * ratingX;
            sumYY += ratingY * ratingY;
            sumXY += ratingX * ratingY;

            numPairs++;
        }

        double numerator = sumXY;
        double denominator = Math.sqrt(sumXX) * Math.sqrt(sumYY);

        double score = 0;

        if (denominator != 0) {
            score = numerator / denominator;
        }

        Map<String, Double> returnVal = new HashMap<>();
        returnVal.put(NUM_PAIRS, numPairs);
        returnVal.put(SCORE, score);

        return returnVal;
    }
}
