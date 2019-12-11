package FinalProject.algorithm;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class DDLof {
    public static class Map_First extends Mapper<Object, Text, IntWritable, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] points = value.toString().split(",");
            double x = Double.parseDouble(points[0]);
            double y = Double.parseDouble(points[1]);
            int id = getPartitionId(x, y);
            context.write(new IntWritable(id), value);
        }
    }

    public static class Reduce_First extends Reducer<IntWritable, Text, NullWritable, Text> {

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
            ArrayList<String> neighborPointsBeforeSort;
            Double kth = 0.0;
            Integer index = 0;
            String p = "";
            int N = k;
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
            int status;

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

                // Get the status of p. If status = 0, it means that p doesn't need to be updated; if status = 1, it means that p needs to be updated.
                if (exL1 == 0.0 && exH1 == 0.0 && exL2 == 0.0 && exH2 == 0.0){
                    status = 0;
                }else{
                    status = 1;
                }
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
                String list_str = StringUtils.join(neighborPointsAfterSort,"+");

                String out = point + ":" + key.get() + ":" + status + ":" + list_str + ":" + kth + ":" + N;
                context.write(NullWritable.get(), new Text(out));
            }

        }
    }

    public static class Map_Second extends Mapper<Object, Text, IntWritable, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] values = value.toString().split(":");
            String point = values[0];
            int coreId = Integer.parseInt(values[1]);
            ArrayList<Integer> suppId = findSuppId(point);
            String out;
            out = "c:" + value.toString();
            context.write(new IntWritable(coreId), new Text(out));

            int supp;
            if(suppId.size()!=0){
                for(int i = 0; i < suppId.size(); i++){
                    supp = suppId.get(i);
                    out = "s:" + value.toString().split(":")[0];
                    context.write(new IntWritable(supp), new Text(out));
                }
            }
        }
    }

    public static class Reduce_Second extends Reducer<IntWritable, Text, NullWritable, Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // get the value of k
            Configuration conf = context.getConfiguration();
            int n = Integer.parseInt(conf.get("k"));

            ArrayList<String> corePointArr = new ArrayList<String>();
            ArrayList<String> suppPointArr = new ArrayList<String>();
            String tag;

            // For each grid, seperate its core point and support point
            for(Text v: values){
                String[] vars = v.toString().split(":");
                tag = vars[0];
                if(tag.equals("c")){
                    corePointArr.add(StringUtils.join(vars,":", 1, vars.length));
                }
                if(tag.equals("s")){
                    suppPointArr.add(vars[1]);
                }
            }

            String[] core;
            for(int m = 0; m < corePointArr.size(); m++){
                Map<String, Double> pointAndDis = new HashMap();
                String str = "";
                core = corePointArr.get(m).split(":");

                int status = Integer.parseInt(core[2]);
                if(status == 0) {
                    str = core[0] + ":" + core[1] + ":" + core[3] + ":" + core[4] + ":" + core[5];
                    context.write(NullWritable.get(), new Text(str));
                }

                else if(status == 1){
                    String neibor = core[3];
                    String[] neibors = neibor.split("[+]");
                    double dis;
                    for(int j = 0; j < neibors.length; j++){
                        dis = calcDist(core[0], neibors[j]);
                        pointAndDis.put(neibors[j], dis);
                    }
                    for(int k = 0; k < suppPointArr.size(); k++){
                        dis = calcDist(core[0], suppPointArr.get(k));
                        pointAndDis.put(suppPointArr.get(k), dis);
                    }

                    ArrayList<Map.Entry<String, Double>> list = new ArrayList<Map.Entry<String, Double>>(pointAndDis .entrySet());
                    Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
                        @Override
                        public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
                            return o1.getValue().compareTo(o2.getValue());
                        }
                    });

                    int N = n;
                    double kth = list.get(n-1).getValue();

                    if(list.size()!=n){
                        for(int p = n; p < list.size(); p++){
                            if(list.get(p).getValue() == kth){
                                N = n+1; // N is the number of KNN
                            }
                        }
                    }

                    String realKNN = list.get(0).getKey();
                    for(int q = 1; q < N; q++){
                        realKNN = realKNN + "+" + list.get(q).getKey() ;
                    }
                    str = core[0] + ":" + core[1] + ":" + realKNN + ":" + kth + ":"+ N;
                    context.write(NullWritable.get(), new Text(str));

                }
            }
        }
    }

    public static class Map_Third extends Mapper<Object, Text, IntWritable, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] values = value.toString().split(":");
            int coreId = Integer.parseInt(values[1]);
            context.write(new IntWritable(coreId), new Text("c:"+values[0]+":"+StringUtils.join(values,":", 2, values.length)));

            ArrayList<Integer> suppId = findSuppId(values[0]);
            int supp;
            if(suppId.size()!=0){
                for(int i = 0; i < suppId.size(); i++){
                    supp = suppId.get(i);
                    context.write(new IntWritable(supp), new Text("s:" + values[0] + ":" + StringUtils.join(values,":", 2, values.length)));
                }
            }
        }
    }

    public static class Reduce_Third extends Reducer<IntWritable, Text, NullWritable, Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            List<Text> cache = new ArrayList<Text>();
            List<Text> value = new ArrayList<Text>();
            Iterator<Text> ite= values.iterator();

            // cache values
            while (ite.hasNext()) {
                Text tmp = ite.next();
                cache.add(new Text(tmp));
                value.add(new Text(tmp));
            }

            Map<String, Double> pointAndKth = new HashMap<String, Double>();
            String[] points;
            String po;
            Double kth;

            for(Text v:value){
                points = v.toString().split(":");
                po = points[1];
                kth = Double.parseDouble(points[3]);
                pointAndKth.put(po, kth);
            }

            int N;
            String[] neibors;

            for(Text v:cache){
                points = v.toString().split(":");
                double sumOfReachDis = 0.0;
                double lrd = 0.0;
                if(points[0].equals("c")){
                    N = Integer.parseInt(points[4]);
                    neibors = points[2].split("[+]");
                    for(int i = 0; i < neibors.length; i++){
                        sumOfReachDis += Math.max(pointAndKth.get(neibors[i]), calcDist(points[1],neibors[i]));
                    }
                    lrd = N / sumOfReachDis;
                    context.write(NullWritable.get(), new Text(key.get()+ ":" + points[1] + ":" + points[2] + ":" + lrd + ":" + points[4]));
                }
            }

        }
    }

    public static class Map_Fourth extends Mapper<Object, Text, IntWritable, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] values = value.toString().split(":");
            int coreId = Integer.parseInt(values[0]);
            context.write(new IntWritable(coreId), new Text("c:" + StringUtils.join(values,":", 1, values.length)));

            ArrayList<Integer> suppId = findSuppId(values[1]);
            int supp;
            if(suppId.size()!=0){
                for(int i = 0; i < suppId.size(); i++){
                    supp = suppId.get(i);
                    context.write(new IntWritable(supp), new Text("s:" + StringUtils.join(values,":", 1, values.length)));
                }
            }
        }
    }

    public static class Reduce_Fourth extends Reducer<IntWritable, Text, Text, DoubleWritable> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            List<Text> cache = new ArrayList<Text>();
            List<Text> value = new ArrayList<Text>();
            Iterator<Text> ite= values.iterator();

            // cache values
            while (ite.hasNext()) {
                Text tmp = ite.next();
                cache.add(new Text(tmp));
                value.add(new Text(tmp));
            }

            Map<String, Double> pointAndlrd = new HashMap<String, Double>();
            String[] points;
            String po;
            Double lrd;
            for(Text v:value){
                points = v.toString().split(":");
                po = points[1];
                lrd = Double.parseDouble(points[3]);
                pointAndlrd.put(po, lrd);
            }

            int N;
            String[] neibors;

            Map<String, Double> pointAndLof = new HashMap<String, Double>();
            for(Text v:cache){
                points = v.toString().split(":");
                double sumOfAvgLrd = 0.0;
                double lof = 0.0;
                if(points[0].equals("c")){
                    N = Integer.parseInt(points[4]);
                    neibors = points[2].split("[+]");
                    for(int i = 0; i < neibors.length; i++){
                        sumOfAvgLrd += pointAndlrd.get(neibors[i]) / pointAndlrd.get(points[1]);
                    }
                    lof = sumOfAvgLrd / N;
                    pointAndLof.put(points[1], lof);
                }
            }


            for(String pt: pointAndLof.keySet()){
                if(pointAndLof.get(pt) > 2){
                    context.write(new Text(pt), new DoubleWritable(pointAndLof.get(pt)));
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

    public static ArrayList<Integer> findSuppId(String p) throws IOException {
        double x = Double.parseDouble(p.split(",")[0]);
        double y = Double.parseDouble(p.split(",")[1]);
        ArrayList<Integer> res = new ArrayList<Integer>();
        FileReader fr = new FileReader("/Users/daojun/Desktop/mapreduce/SuppBound/SuppBound.txt");
        BufferedReader br = new BufferedReader(fr);

        String line;
        String[] arys;
        Integer id;
        String[] suppBounds;
        int l1;
        int h1;
        int l2;
        int h2;
        double exL1;
        double exH1;
        double exL2;
        double exH2;
        double suppL1;
        double suppH1;
        double suppL2;
        double suppH2;
        int status = 0;
        while((line = br.readLine())!= null){
            arys = line.split(":");
            id = Integer.parseInt(arys[0]);
            l1 = (id % 100) * 100; // lower bound of x
            h1 = (id % 100 + 1) * 100; // upper bound of x
            l2 = (id / 100) * 100; // lower bound of y
            h2 = (id / 100 + 1) * 100; // upper bound of y

            suppBounds = arys[1].split(",");
            exL1 = Double.parseDouble(suppBounds[0]);
            exH1 = Double.parseDouble(suppBounds[1]);
            exL2 = Double.parseDouble(suppBounds[2]);
            exH2 = Double.parseDouble(suppBounds[3]);

            if(exL1 == 0.0 && exH1 == 0.0 && exL2 == 0.0 && exH2 == 0.0) status = 1;

            suppL1 = l1 - exL1;
            suppH1 = h1 + exH1;
            suppL2 = l2 - exL2;
            suppH2 = h2 + exH2;

            if(status!= 1 && (x > suppL1 && x < suppH1 && y > suppL2 && y < suppH2) && (!(x > l1 && x < h1 && y > l2 && y < h2))){
                res.add(id);
            }
        }
        br.close();
        fr.close();
        return res;
    }

    public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
        long startTime = System.currentTimeMillis();    //get current time
        JobConf conf = new JobConf(DDLof.class);
        conf.set("k", args[5]);
        // job1
        Job job1 = new Job(conf, "job1");
        job1.setJarByClass(DDLof.class);
        job1.setMapperClass(Map_First.class);
        job1.setReducerClass(Reduce_First.class);

        // IntWritable, Text, NullWritable, Text
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(Text.class);

        ControlledJob ctrljob1 = new ControlledJob(conf);
        ctrljob1.setJob(job1);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        // job2
        Job job2 = new Job(conf, "job2");
        job2.setJarByClass(DDLof.class);
        job2.setMapperClass(Map_Second.class);
        job2.setReducerClass(Reduce_Second.class);

        // IntWritable, Text, NullWritable, Text
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Text.class);

        ControlledJob ctrljob2 = new ControlledJob(conf);
        ctrljob2.setJob(job2);
        ctrljob2.addDependingJob(ctrljob1);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        // job 3
        Job job3 = new Job(conf, "job3");
        job3.setJarByClass(DDLof.class);
        job3.setMapperClass(Map_Third.class);
        job3.setReducerClass(Reduce_Third.class);

        // IntWritable, Text, NullWritable, Text
        job3.setMapOutputKeyClass(IntWritable.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(NullWritable.class);
        job3.setOutputValueClass(Text.class);

        ControlledJob ctrljob3 = new ControlledJob(conf);
        ctrljob3.setJob(job3);
        ctrljob3.addDependingJob(ctrljob2);

        FileInputFormat.addInputPath(job3, new Path(args[2]));
        FileOutputFormat.setOutputPath(job3, new Path(args[3]));

        // job 4
        Job job4 = new Job(conf, "job4");
        job4.setJarByClass(DDLof.class);
        job4.setMapperClass(Map_Fourth.class);
        job4.setReducerClass(Reduce_Fourth.class);

        // IntWritable, Text, Text, DoubleWritable
        job4.setMapOutputKeyClass(IntWritable.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setOutputKeyClass(NullWritable.class);
        job4.setOutputValueClass(DoubleWritable.class);

        ControlledJob ctrljob4 = new ControlledJob(conf);
        ctrljob4.setJob(job4);
        ctrljob4.addDependingJob(ctrljob3);

        FileInputFormat.addInputPath(job4, new Path(args[3]));
        FileOutputFormat.setOutputPath(job4, new Path(args[4]));

        JobControl jobCtrl = new JobControl("myCtrl");

        jobCtrl.addJob(ctrljob1);
        jobCtrl.addJob(ctrljob2);
        jobCtrl.addJob(ctrljob3);
        jobCtrl.addJob(ctrljob4);

        Thread t = new Thread(jobCtrl);
        t.start();

        while(true){
            if(jobCtrl.allFinished()){
                System.out.println(jobCtrl.getSuccessfulJobList());
                jobCtrl.stop();
                long endTime = System.currentTimeMillis();
                System.out.println((endTime-startTime) + "");
                break;
            }
        }
    }
}
