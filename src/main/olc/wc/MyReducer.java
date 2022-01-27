package olc.wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//apple apple ball

//apple [1 1]
//ball [1]

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text word, Iterable<IntWritable> it, Context context)
			throws IOException, InterruptedException {
		System.out.println("Word= "+word.toString());
		System.out.print("[");
		//apple [1 1]
		int runningsum=0;
		int count=0;
		for(IntWritable i:it) {
			System.out.print(i.get()+",");
			runningsum=runningsum+i.get();
			count=count+1;
		}
		System.out.print("]");
		// [1,1,]
		
		context.write(word,new IntWritable(runningsum/count));

	}

}
