package olc.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class MyDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String iPath="hdfs://localhost:9000/user/hduser/columnwiseaveragenumber_input";
		String oPath="hdfs://localhost:9000/user/hduser/columnwiseaveragenumber_output";
		
		Path inputPath=new Path(iPath);
		Path outputPath=new Path(oPath);
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		//Job job = new Job(conf);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setJarByClass(MyDriver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		System.exit(job.waitForCompletion(true)?0:1);
		
		// all link to mapper, reducer,driver program,
		// input path ,output path.

	}

}
