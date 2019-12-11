# Data-Driven Distributed LOF 

[![Build Status](https://travis-ci.org/joemccann/dillinger.svg?branch=master)](https://travis-ci.org/joemccann/dillinger)

### Production Environment 
IntelliJ + Maven

### Localized LOF
In our project, we firstly implemented localized LOF algorithm using only one Map-Reduce job. We have 3 input parameters in this job. The first one is the input data path, the second one is output data path, the third one is the number of k. 

For example, in the run configurations, we set the program arguments as:

```
input_outlier/ output_outlier/ "20"
```

So, in this example, we put the data under the "input_outlier" directory and we export the output to the "output_outlier" directory and we set k as 20.

### Data-Driven Distributed LOF (DDLOF)

According to the paper ***Distributed Local Outier Detection in Big Data***, there are five jobs to implement this algorithm.

They are:

* Job 1: Skew-aware Partitioning
* Job 2: Core Partition KNN search
* Job 3: Support Partition KNN search 
* Job 4: Compute LRD
* Job 5: compute LOF

Instead of using skew-aware partitioning, we simplied the partitioning part and self-defined the partitioning method based on our customized dataset. By simpling this step which is hard to implement, we can still focus on the Data Driven KNN search. Though we simplied this step, we need to do some preparation in the "CalSuppBound.java" before we implement our DDLOF algorithm.

By running the "CalSuppBound.java", we can calculate every grid's extended distances. The output result will be stored in a file which we will later use in our distributed LOF process. In personal computer, the path of output result is:

```
/Users/daojun/Desktop/mapreduce/SuppBound/SuppBound.txt
```

In the "DDLof.java" file, we have four jobs, they are:

* Core Partition KNN search
* Support Partition KNN search 
* Compute LRD
* compute LOF

The output of the first job will be the input of the second job. The output of the second job will be the input of the third job. The output of the third job will be the input of the fourth job. Thus, in the main configuration, we link the four jobs together to run in sequence. 

### Reference
[1]. Breunig, Markus & Kriegel, Hans-Peter & Ng, Raymond & Sander, Joerg. (2000). LOF: Identifying Density-Based Local Outliers.. ACM Sigmod Record. 29. 93-104. 10.1145/342009.335388. 

[2]. Yan, Yizhou & Cao, Lei & Kuhlman, Caitlin & Rundensteiner, Elke. (2017). Distributed Local Outlier Detection in Big Data. 1225-1234. 10.1145/3097983.3098179. 


