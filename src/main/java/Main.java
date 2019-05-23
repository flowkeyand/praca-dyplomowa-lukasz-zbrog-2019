import history_analysis.HistoryAnalysisMap;
import history_analysis.HistoryAnalysisReduce;
import history_analysis.HistoryAnalysisTo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import user_with_max_ratings.MaxMapFirst;
import user_with_max_ratings.MaxReduceFirst;

import java.io.IOException;

import static user_with_max_ratings.MaxTo.MAX;
import static user_with_max_ratings.MaxTo.USER_ID;

public class Main {
    public static void main(String[] args) {
        Path inputMainData = new Path(args[0]);
        Path out = new Path(args[1]);

        try {
            // --------------- USER_WITH_MAX_RATINGS JOB 1 --------------
            Job userWithMaxRatingsJob = Job.getInstance();
            userWithMaxRatingsJob.setJobName("user_with_max_ratings");
            userWithMaxRatingsJob.setJarByClass(Main.class);

            userWithMaxRatingsJob.setOutputKeyClass(Text.class);
            userWithMaxRatingsJob.setOutputValueClass(DoubleWritable.class);

            userWithMaxRatingsJob.setMapperClass(MaxMapFirst.class);
            userWithMaxRatingsJob.setReducerClass(MaxReduceFirst.class);

            userWithMaxRatingsJob.setInputFormatClass(TextInputFormat.class);
            userWithMaxRatingsJob.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(userWithMaxRatingsJob, inputMainData);
            FileOutputFormat.setOutputPath(userWithMaxRatingsJob, new Path(out, "maxOut"));

            userWithMaxRatingsJob.setNumReduceTasks(1);

            userWithMaxRatingsJob.waitForCompletion(true);

            long userId = userWithMaxRatingsJob.getCounters().findCounter("userWithMaxRatingsJob", USER_ID).getValue();
            long max = userWithMaxRatingsJob.getCounters().findCounter("userWithMaxRatingsJob", MAX).getValue();

            // --------------- HISTORY_ANALYSIS JOB 2 --------------
            Configuration conf = new Configuration();
            conf.set(USER_ID, String.valueOf(userId));

            Job historyAnalysisJob = Job.getInstance(conf);
            historyAnalysisJob.setJobName("history_analysis");
            historyAnalysisJob.setJarByClass(Main.class);

            historyAnalysisJob.setOutputKeyClass(HistoryAnalysisTo.class);
            historyAnalysisJob.setOutputValueClass(DoubleWritable.class);

            historyAnalysisJob.setMapperClass(HistoryAnalysisMap.class);
            historyAnalysisJob.setReducerClass(HistoryAnalysisReduce.class);

            historyAnalysisJob.setInputFormatClass(TextInputFormat.class);
            historyAnalysisJob.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(historyAnalysisJob, inputMainData);
            FileOutputFormat.setOutputPath(historyAnalysisJob, new Path(out, "historyOut"));

            historyAnalysisJob.setNumReduceTasks(1);

            historyAnalysisJob.waitForCompletion(true);

        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
