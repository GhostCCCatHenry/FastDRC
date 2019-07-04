package gene.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;

public class App
{
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException {

        long startTime = System.currentTimeMillis();

        Configuration conf =new Configuration();

        //For sequences longer than 128M, the fragment size must be adjusted to 256M.
        conf.set("mapreduce.input.fileinputformat.split.minsize","268435456");
        String Name = args[0];
        Job job =Job.getInstance(conf,Name);

        //Main class name！
        job.setJarByClass(App.class);

        //File that holds the file path
        //File file = new File("F:\\geneExp\\geneName\\chr1_name.fa");
        File file = new File("/home/gene/input/"+Name+"_hdfs_name.fa");//author's file path.
        BufferedReader br = new BufferedReader(new FileReader(file));

        //Add a distributed cache file, which is obtained and processed in the setup method of the map class.
        //job.addCacheFile(new Path("F:\\geneExp\\chr1\\chr1.fa").toUri());
        job.addCacheFile(new Path("hdfs://master:9000/input/"+Name+"/"+Name+".fa").toUri());//hadoop filesystem distributed cache
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapperClass(geneMap.class);
        job.setNumReduceTasks(0);

        //Loop setting custom multi-output class
        String str;
        //Get the input path from the file and define multiple outputs
        while ((str=br.readLine())!=null){
            KeyValueTextInputFormat.addInputPaths(job,str);
            String k1[]=str.split("_");
            String k2[]=k1[4].split("\\.");
            MultipleOutputs.addNamedOutput(job, k2[3]+k2[4], SequenceFileOutputFormat.class,BytesWritable.class, NullWritable.class);
        }

        //Check if there is already an output path on hdfs, if it is, delete it in advance.
        //Path outPath=new Path("F:\\geneExp\\out");
        Path outPath=new Path("/out/"+Name);
        FileSystem fs =FileSystem.get(conf);
        if(fs.exists(outPath)){
            fs.delete(outPath,true);
        }
        FileOutputFormat.setOutputPath(job,outPath);
        job.waitForCompletion(true);
        System.out.println("all maps time consuming" + (System.currentTimeMillis() - startTime) + "ms");
    }
}
