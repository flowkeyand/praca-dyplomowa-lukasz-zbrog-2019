import firstJob.JobFirstTo;
import firstJob.MapFirstOne;
import firstJob.MapFirstTwo;
import firstJob.ReduceFirst;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import secondJob.MapSecond;
import secondJob.ReduceSecond;
import thirdJob.JobThirdTo;
import thirdJob.MapThird;
import thirdJob.ReduceThird;
import utils.ConstantsUtil;

import java.io.IOException;

public class Main {
    public static void main(String[] args) {
        Path inputMainData = new Path(args[0]);
        Path inputAdditionalData = new Path(args[1]);
        String confidence = args[2];
        String mean = args[3];
        String numberOfRows = args[4];
        Path out = new Path(args[5]);

        try {
            // --------------- BAYES_ESTIMATOR JOB 1 --------------
            Job job = Job.getInstance();
            job.setJobName("bayes_estimator");
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

            // --------------- BAYES_ESTIMATOR JOB 2 --------------
            Configuration conf1 = new Configuration();
            conf1.set(ConstantsUtil.CONFIDENCE, confidence);
            conf1.set(ConstantsUtil.MEAN, mean);

            Job job2 = Job.getInstance(conf1);
            job2.setJobName("bayes_estimator_final");
            job2.setJarByClass(Main.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(DoubleWritable.class);

            job2.setMapperClass(MapSecond.class);
            job2.setReducerClass(ReduceSecond.class);

            job2.setInputFormatClass(TextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job2, new Path(out, "out1"));
            FileOutputFormat.setOutputPath(job2, new Path(out, "out2"));

            job2.waitForCompletion(true);

            // --------------- BAYES_ESTIMATOR JOB 3 --------------
            Configuration conf2 = new Configuration();
            conf2.set(ConstantsUtil.NUMBER_OF_ROWS, numberOfRows);

            Job job3 = Job.getInstance(conf2);
            job3.setJobName("bayes_estimator_final_sorted");
            job3.setJarByClass(Main.class);

            job3.setOutputKeyClass(JobThirdTo.class);
            job3.setOutputValueClass(NullWritable.class);

            job3.setMapperClass(MapThird.class);
            job3.setReducerClass(ReduceThird.class);

            job3.setInputFormatClass(TextInputFormat.class);
            job3.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job3, new Path(out, "out2"));
            FileOutputFormat.setOutputPath(job3, new Path(out, "bayesEstimatorOut"));

            job3.setNumReduceTasks(1);

            job3.waitForCompletion(true);

        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
