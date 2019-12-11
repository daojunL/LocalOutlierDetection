package FinalProject.findSupp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class CalSuppBound {
    public static class MyMapper extends Mapper<Object, Text, IntWritable, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] points = value.toString().split(",");
            double x = Double.parseDouble(points[0]);
            double y = Double.parseDouble(points[1]);
            int id = getPartitionId(x, y);
            context.write(new IntWritable(id), value);
        }
    }

    public static class MyReducer extends Reducer<IntWritable, Text, NullWritable, Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Map<String, ArrayList<String>> mapOfPointsAndOtherPoints = new HashMap<String, ArrayList<String>>();
            Map<String, ArrayList<Double>> mapOfPointsAndDistances = new HashMap<String, ArrayList<Double>>();
            ArrayList<String> currPoints = new ArrayList<String>();
            String currP;
            Double disc;

            // get the value of k
            Configuration conf = context.getConfiguration();
            int k = Integer.parseInt(conf.get("k"));

            // get the core area according to key
            int l1 = (key.get() % 10) * 100; // lower bound of x
            int h1 = (key.get() % 10 + 1) * 100; // upper bound of x
            int l2 = (key.get() / 10) * 100; // lower bound of y
            int h2 = (key.get() / 10 + 1) * 100; // upper bound of y

            System.out.println(l1);
            System.out.println(key.get() + "");
            System.out.println(l1 + "");

            // put all the points into a ArrayList
            for(Text v:values){
                currPoints.add(v.toString());
            }

            /**
             * 1. Step One: Create two HashMap: one is mapOfPointsAndOtherPoints, the other one is mapOfPointsAndDistances
             * mapOfPointsAndOtherPoints: key is every point p, value is other points (except point p)
             * mapOfPointsAndDistances: key is every point p, value is the distances between other points and  p
             *
             * @ Author Daojun
             */

            for(int i = 0; i < currPoints.size(); i++){
                currP = currPoints.get(i);
                ArrayList<Double> distances = new ArrayList<Double>();
                ArrayList<String> otherPoints = new ArrayList<String>();
                for(int j = 0; j < currPoints.size(); j++){
                    if(j != i) {
                        otherPoints.add(currPoints.get(j));
                        disc = calcDist(currP, currPoints.get(j));
                        distances.add(disc);
                    }
                }
                mapOfPointsAndOtherPoints.put(currP, otherPoints);
                mapOfPointsAndDistances.put(currP, distances);
            }

            /**
             * 2. Step Two: Calculate the local kNN and local k-distance of points
             * Here we also create two HashMap, mapOfPointAndKthDis & mapOfPointAndNeighborPoints
             * mapOfPointAndKthDis: key is every point p, value is the local k-distance of p
             * mapOfPointAndNeighborPoints: key is every point p, value is the local kNN of p
             *
             * @ Author Daojun
             */

            ArrayList<Double> disList;
            Double kth;
            double pointX;
            double pointY;
            double disL1;
            double disH1;
            double disL2;
            double disH2;
            String[] lines;
            double exL1;
            double exH1;
            double exL2;
            double exH2;
            String suppBound;
            ArrayList<Double> exArrL1 = new ArrayList<Double>();
            ArrayList<Double> exArrH1 = new ArrayList<Double>();
            ArrayList<Double> exArrL2 = new ArrayList<Double>();
            ArrayList<Double> exArrH2 = new ArrayList<Double>();

            for (String point: mapOfPointsAndDistances.keySet()){

                disList = mapOfPointsAndDistances.get(point);
                Map<Integer, Double> mapOfIndexAndDis = new HashMap<Integer, Double>();
                for(int i = 0; i < disList.size(); i++){
                    mapOfIndexAndDis.put(i, disList.get(i));
                }
                ArrayList<Map.Entry<Integer, Double>> listOfIndexAndDis = new ArrayList<Map.Entry<Integer, Double>>(mapOfIndexAndDis.entrySet());
                Collections.sort(listOfIndexAndDis, new Comparator<Map.Entry<Integer, Double>>() {
                    @Override
                    public int compare(Map.Entry<Integer, Double> o1, Map.Entry<Integer, Double> o2) {
                        return o1.getValue().compareTo(o2.getValue());
                    }
                });
                kth = listOfIndexAndDis.get(k-1).getValue();

                lines = point.split(",");

                // Calculate the support bound of every point
                pointX = Double.parseDouble(lines[0]);
                pointY = Double.parseDouble(lines[1]);
                disL1 = pointX - l1;
                disH1 = h1 - pointX;
                disL2 = pointY - l2;
                disH2 = h2 - pointY;

                exL1 = getExtendedDistance(kth, disL1);
                exH1 = getExtendedDistance(kth, disH1);
                exL2 = getExtendedDistance(kth, disL2);
                exH2 = getExtendedDistance(kth, disH2);

                exArrL1.add(exL1);
                exArrH1.add(exH1);
                exArrL2.add(exL2);
                exArrH2.add(exH2);
            }

            Collections.sort(exArrL1);
            Collections.sort(exArrH1);
            Collections.sort(exArrL2);
            Collections.sort(exArrH2);
            Collections.reverse(exArrL1);
            Collections.reverse(exArrH1);
            Collections.reverse(exArrL2);
            Collections.reverse(exArrH2);

            suppBound = key.get() + ":" + exArrL1.get(0) + "," + exArrH1.get(0) + "," + exArrL2.get(0) + "," + exArrH2.get(0);
            context.write(NullWritable.get(), new Text(suppBound));
        }
    }

    public static double calcDist(String p1, String p2){
        String[] xAndYP1 = p1.split(",");
        String[] xAndYP2 = p2.split(",");
        double xP1 = Double.parseDouble(xAndYP1[0]);
        double yP1 = Double.parseDouble(xAndYP1[1]);
        double xP2 = Double.parseDouble(xAndYP2[0]);
        double yP2 = Double.parseDouble(xAndYP2[1]);

        return Math.sqrt((xP1 - xP2) * (xP1 - xP2) + (yP1 - yP2) * (yP1 - yP2));
    }

    public static int getPartitionId(double x, double y){
        int len = 100;
        int num = 1000 / len;
        int indexX = (int) x / len;
        int indexY = (int) y / len;
        int id = indexX + num * indexY;
        return id;
    }

    public static double getExtendedDistance(double r, double dis){
        return Math.max(0, r - dis);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("k", args[2]); // k = 10 in our test
        Job job = new Job(conf, "LOF");
        job.setMapperClass(CalSuppBound.MyMapper.class);
        job.setReducerClass(CalSuppBound.MyReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        //job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
