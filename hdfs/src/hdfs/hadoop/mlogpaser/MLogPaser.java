package hdfs.hadoop.mlogpaser;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * Ê¹ÓÃÀý×Ó
 * @author shi
 *
 */
public class MLogPaser {
	
	public static class AMapper extends Mapper<Text,Text,Text,Text>{

		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
				context.write(value,key);
			
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf);
		job.setJarByClass(MLogPaser.class);
		job.setMapperClass(AMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setNumReduceTasks(0);

        job.setInputFormatClass(MLogFileFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
	}
}
