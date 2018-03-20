package hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @ClassName: WordCountReducer
 * 
 * @Description: TODO
 * 
 * @author kngines
 * 
 * @date 2018年3月17日
 */

public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	//	生命周期：框架每传递进来一个kv 组，reduce方法被调用一次
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		
		int count = 0;  // 定义一个计数器
		for (IntWritable value : values) { // 遍历所有v，并累加到count中
			count += value.get();
		}
		context.write(key, new IntWritable(count));
	}
}