import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class run{

	public static class NBTrainMapper
	       extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text feature = new Text();

		public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {
      
 			Vector<String> tokens, labels;
      			NB_train_hadoop nbTrain = new NB_train_hadoop();
			
			tokens = nbTrain.getDocTokens(value.toString());
			labels = nbTrain.getDocLabels(tokens.get(0));
			
			for (int i = 0; i < labels.size(); i++) {
				for (int j = 1; j < tokens.size(); j++) {
					feature.set("Y="+labels.get(i)+",W="+tokens.get(j));						
					context.write(feature, one); 	
				}
				feature.set("Y="+labels.get(i)+",W=*");				
				context.write(feature, new IntWritable(tokens.size()-1));
				feature.set("Y="+labels.get(i));
				context.write(feature, one); 			
			}
			feature.set("Y=*");
			context.write(feature, new IntWritable(labels.size()));
		}
  	}

  	public static class NBTrainReducer
       		extends Reducer<Text,IntWritable,Text,IntWritable> {
    		
		private IntWritable result = new IntWritable();

    		public void reduce(Text key, Iterable<IntWritable> values,
                	       Context context
                	       ) throws IOException, InterruptedException {
      			int sum = 0;
      			for (IntWritable val : values) {
        			sum += val.get();
      			}
      			result.set(sum);
      			context.write(key, result);
    		}
  	}

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "NBTrain");
    job.setJarByClass(run.class);
    job.setMapperClass(NBTrainMapper.class);
    job.setCombinerClass(NBTrainReducer.class);
    job.setReducerClass(NBTrainReducer.class);
    job.setNumReduceTasks(Integer.parseInt(args[2]));
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
