package olc.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*word1.txt
apple ball apple
ball ball apple

*/

/*0,apple ball apple
15,ball ball apple
*/
public class MyMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	private long colno = 0;
	private String lno = new String();

	public MyMapper() {	colno=0; }

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
	colno=0;

	System.out.println("Key ="+ key.get());
	System.out.println("Value="+ value.toString());
	String words[]=value.toString().split(" ");
	
	for(String word:words) {
		System.out.println("Col No ="+ colno);
		String cno= "col" + Long.toString(colno);
		System.out.println("Col Name ="+ cno);

		System.out.println("Output Key ="+ cno);
		System.out.println("Output 	Value="+ word);

		context.write(new Text(cno),new IntWritable(Integer.parseInt(word)));
		colno++;
	}
	}
}


//Mapper output
/*
 apple 1
  ball 1
  apple 1
 
ball 1
ball 1
apple 1
*/

//shuffle data

/*
 apple 1
 ball 1
 apple 1
ball 1
ball 1
apple 1
*/

//Sorted
/*
 apple 1
 apple 1
 apple 1

ball 1
ball 1
ball 1
*/

//Group By
//----------------
/*
apple [1,1,1]
ball [1,1,1]
*/
