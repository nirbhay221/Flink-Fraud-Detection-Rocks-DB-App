# PROJECT OVERVIEW:

## Fraud Detection with Flink

This project is an implementation of a fraud detection system using the Apache Flink stream processing framework. It includes two Java classes, FraudDetectionJob and FraudDetector, which are responsible for the data processing and fraud detection logic, respectively.

### FraudDetectionJob

The FraudDetectionJob class is the main class of the project, which sets up the Flink execution environment and defines the processing pipeline. Here is a brief overview of its functionality:
<ol>
    <li>Set up the Flink execution environment and enable checkpointing for fault tolerance.</li>
    <li>Create a RocksDB state backend and set it as the state backend for the environment.</li>
    <li>Set the event time as the time characteristic for the environment.</li>
    <li>Add a transaction source to read transactions from.</li>
    <li>Use a keyed process function to assign manual timestamps based on the account ID.</li>
    <li>Use a tumbling window of 5 minutes to aggregate transactions per account.</li>
    <li>Apply the fraud detection logic on the aggregated transactions.</li>
    <li>Send the alerts to the alert sink.</li>
    <li>Execute the Flink job.</li>
    <li>The FraudDetectionJob class also includes a TimestampAssigner nested class that extends the KeyedProcessFunction class. Its processElement method extracts the account ID and transaction amount from each transaction and emits them as a tuple. Its onTimer method is currently unused.</li>
</ol>

### FraudDetector

The FraudDetector class is responsible for the fraud detection logic, which is applied to the aggregated transactions in step 7 of the FraudDetectionJob. Here is a brief overview of its functionality:

<ol>
    <li>Create a state descriptor for the transaction state, which is keyed by the account ID and holds a list of transaction amounts.</li>
    <li>Create a state descriptor for the last transaction time state, which is keyed by the account ID and holds the timestamp of the last transaction.</li>
    <li>Define a threshold for the maximum allowed transaction amount within a window of time.</li>
    <li>Process each transaction in the input stream, updating the transaction state and last transaction time state.</li>
    <li>Check if the sum of the transaction amounts in the window exceeds the threshold. If it does, emit an alert.</li>
    <li>The FraudDetector class also includes a number of metrics for monitoring the fraud detection process using the Prometheus monitoring system. These metrics include a TransactionCounter and a AlertCounter, which count the total number of transactions and alerts processed, respectively. There is also a TransactionGauge, which tracks the number of transactions currently being processed, and a AlertGauge, which tracks the number of alerts currently being processed.</li>
</ol>

### Conclusion

Overall, this project provides a robust implementation of a fraud detection system using the Flink stream processing framework. Its use of a stateful KeyedProcessFunction allows it to maintain state across multiple transactions, making it suitable for real-time, high-volume data processing. Its integration with the Prometheus monitoring system provides visibility into the performance of the fraud detection process, allowing for quick identification of issues and optimization opportunities.

# HOW TO INSTALL AND RUN THE DEMO:

### Step 1:
- Download/Clone the repository and go to the "master" folder (https://cs551-gitlab.bu.edu/cs551/spring23/group-1/team-1/-/tree/main/master) which is a single project folder that contains codes for Sliding and Tumbling windows w/slicing and record buffer approach.

### Step 2:
- Open the already downloaded and extracted Apache Flink folder (we have used Flink Version 1.16.0) in your system and go to folder named conf -> open flink-conf.yaml
- In that file, search for "state.backend" and set it to: state.backend: rocksdb
- Again, in that file, search for "state.checkpoints.dir" and set it to the rocksdb_data folder path (included inside the cloned/downloaded folder). <br>
  For example, state.checkpoints.dir: file:///home/user/Desktop/master/rocksdb_data

### Step 3:
Inside the AppBase.java, also replace and update the file paths with the rocksdb_data folder path. <br>
For example, <br>pathToRocksDBCheckpoint = "file:///home/user/Desktop/master/rocksdb_data";

**Note**: If the rocksdb_data directory is not present, create a new rocksdb_data directory.

### Step 4:
- Start a Flink cluster.
- Suppose you want to run the "InorderSlidingWindow", then run the following commands in your Terminal: <br>

In Terminal Do the following:

    mvn clean install
    mvn -f pom-InorderSlidingWindow.xml package
    (change file name as required if TumblingWindow then it is mvn -f pom-TumblingWindow.xml package)

- You can run any of the demo folders by just updating the "mvn -f pom-_.xml package" of that folder's pom file name.
- This will create .jar file in the target folder. Upload and submit the "frauddetection-0.1.jar" in your running Apache Flink cluster and your job should be running.

### Step 5: Parallelism:
To run the project folders with parallelism more than 1, we have to slightly configure our flink-conf.yaml. To make things easy, copy the below linked flink-conf.yaml file and replace your current flink-conf.yaml file with this new configured one:

https://cs551-gitlab.bu.edu/cs551/spring23/group-1/team-1/-/blob/main/TumblingWindowParallelism/flink-conf.yaml

Make sure to change the "state.checkpoints.dir" directory to your checkpoints directory.

### Step 6: Prometheus (OPTIONAL):
- In order to compare the custom Sliding Window with the default Flink's implementation of Sliding Window, we use Prometheus to generate graphs. You can find the directory of the following below:
https://cs551-gitlab.bu.edu/cs551/spring23/group-1/team-1/-/tree/main/CustomWindowAssigner

- Also, we will have to configure our flink-conf.yaml file to setup Prometheus. To make things easy, copy the below linked flink-conf.yaml file and replace your current flink-conf.yaml file with this new configured one:
https://cs551-gitlab.bu.edu/cs551/spring23/group-1/team-1/-/blob/main/CustomWindowAssigner/flink-conf.yaml

- If the above linked flink-conf.yaml file does not work, add the following to your flink-conf.yaml file

File:

    metrics.reporters: prom
    metrics.reporter.prom.class: org.apache.flink.metrics.prometheus.PrometheusReporter
    metrics.reporter.prom.port: 8080

- Also, for adding the dependencies for Prometheus to the Flink implementation, use the following pom.xml file:
https://cs551-gitlab.bu.edu/cs551/spring23/group-1/team-1/-/blob/main/master/pom-InorderSlidingWindowGraph.xml

To further configure Prometheus for a Flink application, you need to perform the following steps:

1. Flink to expose metrics to Prometheus, you need to create a flink-conf.yaml configuration file with the following settings:

  Flink-conf.yaml:

    metrics.reporter.promgateway.class: org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporter

    metrics.reporter.promgateway.host: <prometheus_pushgateway_host>

    metrics.reporter.promgateway.port: <prometheus_pushgateway_port>

    metrics.reporter.promgateway.jobName: <job_name>

    metrics.reporter.promgateway.randomJobNameSuffix: true

    metrics.reporter.promgateway.deleteOnShutdown: true

    metrics.reporter.promgateway.interval: 1m

2. Replace <prometheus_pushgateway_host> with the hostname or IP address of the Prometheus Pushgateway server, <prometheus_pushgateway_port> with the port number that the Pushgateway is listening on, and <job_name> with a unique identifier for your Flink job.

    The randomJobNameSuffix setting appends a random suffix to the job name to prevent naming conflicts in case of multiple Flink jobs reporting metrics to the same Pushgateway.

    The deleteOnShutdown setting deletes the metrics pushed to the Pushgateway when the Flink job is shut down.

    The interval setting specifies the reporting interval for Flink metrics. In this example, it is set to 1 minute.

3. Install and configure a Prometheus server. You can download the latest version of Prometheus from the official Prometheus website. Once you have downloaded and extracted the Prometheus files, you need to create a prometheus.yml configuration file with the following settings:

  Settings (Prometheus.yml):
    
    global:

      scrape_interval: 15s

      evaluation_interval: 15s

    scrape_configs:

      - job_name: 'flink'

        scrape_interval: 15s

        static_configs:

          - targets: ['<flink_jobmanager_host>:<flink_jobmanager_port>']


4. Replace <flink_jobmanager_host> with the hostname or IP address of your Flink JobManager, and <flink_jobmanager_port> with the port number that the JobManager is listening on.

5. Start the Prometheus server by running the prometheus binary. Prometheus will start scraping metrics from Flink based on the configuration in prometheus.yml.

6. Access the Prometheus web UI by opening a web browser and navigating to http://localhost:9090. You should see a graph of the metrics scraped from Flink.
By default, Flink exposes a set of metrics through the Prometheus metrics reporter. You can also create custom metrics in your Flink application and expose them to Prometheus by using the Flink Metrics API.

7. Start your flink jar file

8. Flink will then start reporting metrics to Prometheus via the Pushgateway.

**Caution**: In the M1/M2 versions of Mac, the cluster won't be able to load native RocksDB library and will give exceptions/errors. So, the only possible solution for this is using a Virtual Machine (VM). Parallels, UTM, etc. are some of the VM's preferred for M1/M2 Macs. 
