package FinalProject.prepare;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.*;

public class LofLocal {

    public static class MyMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new Text(1+""), value);
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, DoubleWritable> {

        Map<String, ArrayList<String>> mapOfPointsAndOtherPoints = new HashMap<String, ArrayList<String>>();
        Map<String, ArrayList<Double>> mapOfPointsAndDistances = new HashMap<String, ArrayList<Double>>();
        Map<String, Double> mapOfPointAndKthDis = new HashMap<String, Double>();
        Map<String, ArrayList<String>> mapOfPointAndNeighborPoints = new HashMap<String, ArrayList<String>>();
        Map<String, Integer> mapOfPointAndNeighborPointNumber = new HashMap<String, Integer>();
        Map<String, Double> mapOfPointAndLrd = new HashMap<String, Double>();
        Map<String, Double> mapOfPointAndLof = new HashMap<String, Double>();
        ArrayList<String> currPoints = new ArrayList<String>();
        String currP = "";
        Double disc = 0.0;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // get the value of k
            Configuration conf = context.getConfiguration();
            int k = Integer.parseInt(conf.get("k"));

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
             * 2. Step Two: Calculate the k-th distance of the point p & get its nearest neighbor points' coordinates
             * Here we also create two HashMap, mapOfPointAndKthDis & mapOfPointAndNeighborPoints
             * mapOfPointAndKthDis: key is every point p, value is the k-th distance of p
             * mapOfPointAndNeighborPoints: key is every point p, value is the nearest neighbor points of p.
             *
             * @ Author Daojun
             */

            ArrayList<Double> disList;
            ArrayList<String> neighborPointsBeforeSort;
            Double kth = 0.0;
            Integer index = 0;
            String p = "";
            int N = k;

            for (String point: mapOfPointsAndDistances.keySet()){
                disList = mapOfPointsAndDistances.get(point);
                neighborPointsBeforeSort = mapOfPointsAndOtherPoints.get(point);
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
                mapOfPointAndKthDis.put(point, kth);
                for(int i = k; i < listOfIndexAndDis.size(); i++){
                    if(listOfIndexAndDis.get(i).getValue() == kth){
                        N = k+1;
                    }
                }
                ArrayList<String> neighborPointsAfterSort = new ArrayList<String>();
                for(int i = 0; i < N; i++){
                    index = listOfIndexAndDis.get(i).getKey();
                    p = neighborPointsBeforeSort.get(index);
                    neighborPointsAfterSort.add(p);
                }
                mapOfPointAndNeighborPoints.put(point, neighborPointsAfterSort);
                mapOfPointAndNeighborPointNumber.put(point, N);
            }

            /**
             * 3. Step Three: Calculate the local reachability density of every point p
             *
             * @ Author Daojun
             */

            ArrayList<String> neibors;
            Double kthDistofNeibor;
            double pToNeiborDist;
            double pLrd;
            for(int i = 0; i < currPoints.size(); i++){
                neibors = mapOfPointAndNeighborPoints.get(currPoints.get(i));
                double pReachDistOfNeibors = 0.0;
                for(int j = 0; j < neibors.size(); j++){
                    kthDistofNeibor = mapOfPointAndKthDis.get(neibors.get(j));
                    pToNeiborDist = calcDist(currPoints.get(i),neibors.get(j));
                    pReachDistOfNeibors += Math.max(kthDistofNeibor,pToNeiborDist);
                }
                pLrd = mapOfPointAndNeighborPointNumber.get(currPoints.get(i)) / pReachDistOfNeibors;
                mapOfPointAndLrd.put(currPoints.get(i), pLrd);
            }

            /**
             * 4. Step Four: Calculate the local outlier factor of every point p
             *
             * @ Author Daojun
             */

            double lof;
            for(int i = 0; i < currPoints.size(); i++){
                neibors = mapOfPointAndNeighborPoints.get(currPoints.get(i));
                double avgRatioOfLrd = 0.0;
                for(int j = 0; j < neibors.size(); j++){
                    avgRatioOfLrd += mapOfPointAndLrd.get(neibors.get(j)) / mapOfPointAndLrd.get(currPoints.get(i));
                }
                lof = avgRatioOfLrd / mapOfPointAndNeighborPointNumber.get(currPoints.get(i));
                mapOfPointAndLof.put(currPoints.get(i), lof);
            }

            for(String po: mapOfPointAndLof.keySet()){
                if(mapOfPointAndLof.get(po) > 2){
                    context.write(new Text(po), new DoubleWritable(mapOfPointAndLof.get(po)));
                }
            }
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

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();    //get current time
        Configuration conf = new Configuration();
        conf.set("k", args[2]);
        Job job = new Job(conf, "LocalLOF");
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if(job.waitForCompletion(true)){
            long endTime = System.currentTimeMillis();    //get current time
            System.out.println((endTime-startTime) + "");
            System.exit(0);
        }
        //System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
