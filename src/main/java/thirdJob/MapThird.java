package thirdJob;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.ConstantsUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MapThird extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splitLine = value.toString().trim().split(ConstantsUtil.HASH_SPLITTER);
        String movieRatings = splitLine[1];

        ArrayList<JobThirdTo> movieRatingList = new ArrayList<>();
        String[] splitMovieRatings = movieRatings.trim().split(ConstantsUtil.SEMICOLON_SPLITTER);
        for (String data : splitMovieRatings) {
            String[] splitedData = data.trim().split(ConstantsUtil.SLASH_SPLITTER);

            String movieId = splitedData[0];
            String rating = splitedData[1];
            String movieName = splitedData[2];
            movieRatingList.add(new JobThirdTo(movieId, rating, movieName));
        }

        List<JobThirdTo> movieRatingList2 = new ArrayList<>(movieRatingList);

        for (int i = 0; i < movieRatingList.size(); i++) {
            String movieId1 = movieRatingList.get(i).getMovieId().toString();
            String rating1 = movieRatingList.get(i).getRating().toString();
            String movieName1 = movieRatingList.get(i).getMovieName().toString();

            for (int j = i + 1; j < movieRatingList2.size(); j++) {
                String movieId2 = movieRatingList2.get(j).getMovieId().toString();
                String rating2 = movieRatingList2.get(j).getRating().toString();
                String movieName2 = movieRatingList2.get(j).getMovieName().toString();

                Text outKey1 = new Text(movieId1 + ConstantsUtil.COMMA_SPLITTER + movieName1 + ConstantsUtil.COMMA_SPLITTER + movieId2 + ConstantsUtil.COMMA_SPLITTER + movieName2);
                Text outVal1 = new Text(rating1 + ConstantsUtil.COMMA_SPLITTER + rating2);

                Text outKey2 = new Text(movieId2 + ConstantsUtil.COMMA_SPLITTER + movieName2 + ConstantsUtil.COMMA_SPLITTER + movieId1 + ConstantsUtil.COMMA_SPLITTER + movieName1);
                Text outVal2 = new Text(rating2 + ConstantsUtil.COMMA_SPLITTER + rating1);

                context.write(outKey1, outVal1);
                context.write(outKey2, outVal2);
            }
        }
    }
}
