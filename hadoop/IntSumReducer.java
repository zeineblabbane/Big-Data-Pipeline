package tn.insat;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IntSumReducer
        extends Reducer<Text,FloatWritable,Text,FloatWritable> {

    private FloatWritable result = new FloatWritable();

    public void reduce(Text key, Iterable<FloatWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
        int sum = 0;
        int totalinstance = 0;
        for (FloatWritable val : values) {
            System.out.println("value: "+val.get());
            sum += val.get();
            totalinstance=totalinstance+1;
        }
        System.out.println("--> Sum = "+sum);
        result.set((float)(sum/48));
        context.write(key, result);
    }
}
