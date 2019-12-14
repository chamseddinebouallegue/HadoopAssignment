import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MapReduce {

    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub

        Configuration conf = new Configuration();

        conf.set("attributename", args[2]);

        Job job = Job.getInstance(conf, "Find Minimum and Maximum");
        job.setJarByClass(MapReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(calculateMapper.class);
        job.setCombinerClass(combiner.class);
        job.setReducerClass(calculateReducer.class);

        job.setInputFormatClass(NewFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class calculateMapper extends Mapper<LongWritable, Text, Text, Text> {
        Text counter  = new Text("0");
        Text t1 = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            int col = 1;
            Configuration config = context.getConfiguration();
            String attributename = config.get("attributename");
            if (attributename.equals("cpu")) {
                col = 0;
            } else if (attributename.equals("networkin")) {
                col = 1;
            } else if (attributename.equals("networkout")) {
                col = 2;
            } else if (attributename.equals("memory")) {
                col = 3;
            } else if (attributename.equals("target")) {
                col = 4;
            }
            String[] lines = value.toString().split("\n");
            String out ="";
            for(int j = 0; j <lines.length;j++){
                String[] colvalue = lines[j].toString().split(",");
                out = out.concat(colvalue[col]+",");
            }

            String b = String.valueOf(counter);
            int i = Integer.valueOf(b);
            i++;

            counter.set(String.valueOf(i));
            context.write(counter, new Text(out));
        }
    }
    public static class combiner extends Reducer<Text, Text, Text, Text>
    {
        public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException
        {
            Iterator<Text> iterator = values.iterator();
            String value = (iterator.next()).toString();
            String[] val = value.split(",");
            double min = Integer.MAX_VALUE, max = 0;

            for (int i = 0; i<(val.length-1);i++){
                Double doubleVal = Double.valueOf(val[i]);
                if (doubleVal < min) {
                    min = doubleVal;
                }

                //Finding max value
                if (doubleVal > max) {
                    max = doubleVal;
                }
            }

            List<Double> list = new ArrayList<Double>();
            List<Double> Samples = new ArrayList<Double>();
            context.write(new Text("1"), new Text(max+","+min));
        }
    }

    public static class calculateReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            double min = Integer.MAX_VALUE, max = 0;
            Iterator<Text> iterator = values.iterator();

            while (iterator.hasNext()) {
                String value = (iterator.next()).toString();
                String[] val = value.split(",");
                Double maxVal = Double.valueOf(val[0]);
                Double minVal = Double.valueOf(val[1]);
                //Finding min valu
                if (minVal < min) {
                    min = minVal;
                }

                //Finding max value
                if (maxVal > max) {
                    max = maxVal;
                }
            }
            context.write(new Text("1"), new Text("min is:" + min + "  max is:" + max));
        }
    }
}
