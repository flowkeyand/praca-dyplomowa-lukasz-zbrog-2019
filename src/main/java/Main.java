import firstJob.JobFirstTo;
import firstJob.MapFirstOne;
import firstJob.MapFirstTwo;
import firstJob.ReduceFirst;
import fourthJob.JobFourthTo;
import fourthJob.MapFourth;
import fourthJob.ReduceFourth;
import secondJob.JobSecondTo;
import secondJob.MapSecond;
import secondJob.ReduceSecond;
import thirdJob.MapThird;
import thirdJob.ReduceThird;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import utils.ConstantsUtil;

import java.io.IOException;

public class Main {
    public static void main(String[] args) {
        Path inputMainData = new Path(args[0]);
        Path inputAdditionalData = new Path(args[1]);
        String targetFilm = args[2];
        String minNumPairs = args[3];
        String minScore = args[4];
        String numberOfRows = args[5];
        Path out = new Path(args[6]);

        try {
            // --------------- CONGRUENT_FILMS JOB 1 --------------
            Job job = Job.getInstance();
            job.setJobName("congruent_films");
            job.setJarByClass(Main.class);

            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(JobFirstTo.class);

            job.setMapperClass(MapFirstOne.class);
            job.setMapperClass(MapFirstTwo.class);
            job.setReducerClass(ReduceFirst.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            MultipleInputs.addInputPath(job, inputMainData, TextInputFormat.class, MapFirstOne.class);
            MultipleInputs.addInputPath(job, inputAdditionalData, TextInputFormat.class, MapFirstTwo.class);
            FileOutputFormat.setOutputPath(job, new Path(out, "out1"));

            job.waitForCompletion(true);

            // --------------- CONGRUENT_FILMS JOB 2 --------------
            Job job2 = Job.getInstance();
            job2.setJobName("congruent_films_user_ratings");
            job2.setJarByClass(Main.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(JobSecondTo.class);

            job2.setMapperClass(MapSecond.class);
            job2.setReducerClass(ReduceSecond.class);

            job2.setInputFormatClass(TextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job2, new Path(out, "out1"));
            FileOutputFormat.setOutputPath(job2, new Path(out, "out2"));

            job2.waitForCompletion(true);

            // --------------- CONGRUENT_FILMS JOB 3 --------------
            Configuration conf = new Configuration();
            conf.set(ConstantsUtil.TARGET_FILM, targetFilm);
            conf.set(ConstantsUtil.MIN_NUMBER_PAIRS, minNumPairs);
            conf.set(ConstantsUtil.MIN_SCORE, minScore);
            conf.set(ConstantsUtil.NUMBER_OF_ROWS, numberOfRows);

            Job job3 = Job.getInstance(conf);
            job3.setJobName("congruent_films_cosine_similarity");
            job3.setJarByClass(Main.class);

            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(Text.class);

            job3.setMapperClass(MapThird.class);
            job3.setReducerClass(ReduceThird.class);

            job3.setInputFormatClass(TextInputFormat.class);
            job3.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job3, new Path(out, "out2"));
            FileOutputFormat.setOutputPath(job3, new Path(out, "out3"));

            job3.waitForCompletion(true);

            // --------------- CONGRUENT_FILMS JOB 4 --------------
            Job job4 = Job.getInstance(conf);
            job4.setJobName("congruent_films_sorted_final");
            job4.setJarByClass(Main.class);

            job4.setOutputKeyClass(JobFourthTo.class);
            job4.setOutputValueClass(NullWritable.class);

            job4.setMapperClass(MapFourth.class);
            job4.setReducerClass(ReduceFourth.class);

            job4.setInputFormatClass(TextInputFormat.class);
            job4.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job4, new Path(out, "out3"));
            FileOutputFormat.setOutputPath(job4, new Path(out, "congruentFilmsOut"));

            job4.setNumReduceTasks(1);

            job4.waitForCompletion(true);

        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
