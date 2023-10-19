# Window evaluation strategies in Flink using the RocksDB state backend
                

### Problem Statement

1.1
In this project, We intend to design and implement two alternative window operators in Flink using the Record buffer and Slicing strategies. In our approach, we have opted to include one commutative operator and one holistic operator. Our Objective is  to design efficient operators so that they provide low latency and high throughput.

1.2 
The design and implementation of window aggregation operators have a significant impact on the performance and accuracy of stream processing applications. Therefore, it is crucial to address this problem, and our solution involves several key elements.

Firstly, we utilize a record buffer for efficient data ingestion and handling of both in-order and out-of-order data. Secondly, we employ a Slicing strategy to independently divide the data within each segment, which optimizes the processing efficiency.

Furthermore, to represent the state of the operators, we use key-value pairs in RocksDB. This approach significantly improves the processing speed and memory consumption. RocksDB uses a block-based data structure to store data on disk, which enables efficient memory utilization by minimizing memory fragmentation and reducing the number of disk I/O operations. This results in faster read and write speeds, and lower memory consumption, which can improve processing speed. To verify our idea, we evaluate the performance of different state backends, we use a benchmarking tool which is Apache Flink's Stateful Stream Processing Benchmark. This benchmarking tool provides a set of standard benchmarks to compare the performance of different state backends on various workloads and cluster configurations. We are testing the three major state backends for Apache Flink. They are Memory State Backend, Fs State Backend and RocksDB State Backend.  The benchmarking tool shows Rocks DB has the highest throughput and low latency compared to other state backends, while providing durability and fault tolerance. Even though using RocksDB can incur some overhead, but the benefits it provides often outweigh this overhead. 

Evaluating the performance of our new operators and comparing them with Flink's default implementation across different window types and workload characteristics is essential. This evaluation will help highlight the limitations of each approach and provide insights into how to optimize window processing for different scenarios, making it dynamically adaptable to workload characteristics. Overall, our approach offers a valuable contribution to stream processing applications and presents the potential for further advancements in this field.

1.3
Our window operators hold significant potential for stream processing users, who can leverage them to analyze data streams more efficiently and effectively. Additionally, the benefits of open-sourcing our code extend to developers who can not only use but also improve our window operators to suit their specific needs. Overall, our work presents a valuable contribution to the stream processing community and offers the possibility of further innovations and advancements in this field.

### Proposed Solution

We are planning to implement two window operators for processing an input stream of events, namely slicing and record buffer. Our goal is to demonstrate the benefits of using slicing and record buffer approaches for stream processing in comparision to only using windows without any slicing. We will compare the throughput, latency, fault tolerance and performance metrics of windows with slicing approach, windows without slicing and with record buffer approach.

We plan to implement these approaches on Apache Flink, and use RocksDB as the backend for data processing. We intend to utilize the LSM tree-based functionality of RocksDB, which allows for a high write ingestion capacity at a low cost, and can be further updated. We will use Java as the programming language for this project.

We plan to use two types of operators, namely commutative and holistic, for calculating the mean and median of the data input stream values, respectively. For commutative operators, we will store the partial aggregates of the slices, while for holistic operators, we will store the events as well.

*For the slicing approach*, 
- We will ingest the incoming events on a buffer, the size of which will be proportional to the rate of the incoming stream of data. The window sate will be stored in RocksDB. We will store the tuples as the value and sliceId as the key. We will store the windowId as key and the slices as value too in RocksDB. We will maintain metadata and other pointers to track the multiple sliding windows. 

- The stream slicer will initialize new slices when new events arrive in the buffer stream. Each slice will have a fixed-length time interval, and each event/tuple will belong to only one slice. When each tuple arrives it is stored in RocksDB database. As a slice is updated, it will be stored in RocksDB as a pair with the key as the paneId(sliceId) and the value as the events or the partial aggregates. 

- The slice manager will handle operations like split, update, and merge of the slices. In case of out-of-order tuples, it will compute the partial aggregates. The window manager will compute the final aggregates for the window from the partial aggregates of the slices. It will perform the operations required for processing the queries across windows. In case of updates or changes to the slices, the slice manager will inform it, and it will recompute the aggregates from the slices. When the window is completed, it will assign the slices to the windows and store the windowId as the key and the paneIds(sliceIds) as the value in RocksDB. It will handle the watermarks and maintain the window metadata for the sliding/tumbling windows. There is a many-to-many relation between windows and slices.

*For the record buffer approach*, 
- we will store events ordered by timestamp in a buffer. When the window is triggered, the operator will scan the buffer and retrieve all records that fall within the window interval bounds. The event records will be stored in RocksDB, with the key as the windowId and the events as the value. We can calculate the aggregates on the window tuples.

For Low Latency : 

1. Key Partition for Parallelism : 
To efficiently process large volumes of data, we will use the keyBy operator to partition the sensor stream based on the sensor ID as the key. This will enable us to compute the partial aggregate for each sensor independently. By partitioning the stream based on the key, we can maximize parallelism, as each window can be processed independently for each sensor.
We belive key partitioning is suitable for in-order tuples, and stream slicing for out-of-order tuples to ensure fault tolerance and to maintain correct results.

2. Stateful triggers can avoid unecessary processing of data .

3. Incremental aggregates :Incremental processing allows the computation of the result of aggregation incrementally when the new data arrives.

4. Using Load Balancing and Efficient Serialization techniques.

5. Reducing Memory consumption using Data Compression - using Snappy , Gzip

A crude map 

Data Stream - > Flink (Partitioned across worker nodes in the cluster) -- > Windowing -- > Slicing -- > Window Aggregator -- > RocksDB(Backend) -- > Sink

### Expectations

3.1
By implementing the tumbling window in our solution, we expect to significantly improve the efficiency of the stream processing system. This is because the data stream will be divided into non-overlapping windows of a fixed size, allowing us to process only the events within each window instead of processing every event in the stream. As a result, the amount of data that needs to be stored and processed will be significantly reduced, leading to improved system performance.

Since we intend to use RocksDB as our storage backend we get a persistent and scalable approach to storing aggregate results. When dealing with large volumes of data, RocksDB enables Flink to store the aggregated result on disk for processing intermediate and final results efficiently and reliably.

With the incremental aggregates our approach can manage to reduce memory consumption and improve the accuracy of the system by avoiding recomputation of aggregates.

Overall, we believe that our approach, combining the tumbling window, M4 aggregation technique, RocksDB storage backend, and incremental aggregates approach, will outperform alternative approaches in terms of throughput, latency, memory consumption, and scalability.

3.2 
To achieve better scalability, lower latency, and fault-tolerance for real-time data streams, we can consider using record buffers like Apache Kafka instead of tuple buffers. Building upon the previous architecture, we can adopt a hybrid approach that leverages stateful operators and in-memory data structures, instead of relying solely on RocksDB for persistent storage. This can help reduce latency, improve throughput, and reduce memory usage.

To ensure fault-tolerance, we can implement checkpoints while using in-memory data structures, and explore the use of shared-memory databases to improve the general slicing instead of a single aggregate store. Additionally, we can enhance the benefits of holistic aggregation while mitigating the impact of out-of-order tuples. These optimizations can significantly improve the efficiency, performance, and scalability of the stream processing system.

### Experimental Plan:

4.1 
To validate our hypothesis, we need to measure the following metrics: throughput, latency, memory usage, and fault tolerance, and compare our approach with other techniques. We plan to conduct simulations using different window sizes, buffer sizes, and slicing intervals, and also vary parallelism, RocksDB configuration, and hardware configurations to study the impact on performance.

We measures the latency by recording the timestamp of each event and the time it is processed by the system. Computing the latency as the difference between these two times. We measure the throughput by counting the number of events processed by the system within a fixed time window, such as one minute. We then computes the throughput as the ratio of the number of events processed to the length of the time window. For falut tolerance, we introduce a a new metric called "fault tolerance factor" (FTF) which measures the ratio of the number of processed events after a failure to the total number of events that would have been processed without the failure. We measure the FTF by injecting faults into the system and comparing the number of processed events in the presence of faults to the number of processed events in the absence of faults. The effect of fault tolerance is guaranteed because the state of the operator is periodically checkpointed to the disk using the RocksDB instance. Checkpointing is the process of persisting the state of the operator to disk, so that in case of a failure, the state can be recovered from the last checkpoint. When a failure occurs, the operator can be restarted from the last checkpoint, which ensures that any lost state can be recovered. Lastly, for memory usage, we measure it by monitoring the total memory consumption of the operator's JVM process during its execution. 


We will compare our stream slicing approach with alternative techniques, such as Aggregate Trees, Buckets, Tuple Buffers, Cutty, Pairs, and General Stream Slicing, based on throughput for context-free windows with in-order processing and the effect of out-of-order tuples and context-aware windows on throughput. We will also compare memory consumption for time-based and count-based windows and evaluate scalability and fault tolerance. Furthermore, we will study the impact of workload characteristics, such as stream order, aggregation functions, window types, and measures, on the performance of both window aggregation operator approaches and existing techniques.Overall, these measurements will help us determine the effectiveness and limitations of our approach and allow us to optimize and tune it for different use cases.

To compare these techniques, we will use the Flink-Benchmarks suite, which provides a set of predefined benchmarks. In order to make this comparison fair, we ensure that each windowing technique is evaluated under the same experimental conditions. For example, we will use the same dataset, hardware, and software configurations for each technique. We will also use the same implementation language, libraries, and tools to minimize differences due to implementation details. To be more specific, for configuring the experimental conditions, such as the dataset size, the number of processing nodes, the number of parallel tasks, and the window sizes. For example, We will use a dataset of 1 million records, a cluster of 4 machines, 4 parallel tasks, and a window size of 10 seconds. For implementing the windowing techniques in Flink, we ensure that using the same programming language, libraries, and tools. For example, we will use Java and the Flink Streaming API. For benchmark conditions, we will use Flink-Benchmarks to run the benchmarks for each windowing technique under the same experimental conditions. For example, we will use the "Throughput" benchmark, which measures the number of records processed per second. 

4.2 
At the outset, we will be utilizing a dataset that comprises a subset of traces obtained from a large Google cluster of 12.5k machines

4.3 
To conduct our experiments, we will begin by deploying on a local machine and then later scale to the cloud. We have determined that a local machine will suffice for our initial stage. Throughout our experiments, we will report on the performance metrics of throughput, latency, and memory consumption. 
To measure throughput, we will use the Yahoo Streaming Benchmark implementation for Apache Flink. For latency measurements, we will use the JMH benchmarking suite, which provides precise latency measurements on JVM-based systems. Lastly, we will measure memory consumption using Java VisualVM, a visual tool that comes with the Java Development Kit (JDK) and allows us to monitor the memory usage of our Java applications.

4.4 
The measurements we collect, such as throughput, latency, and memory consumption, will serve as evidence to support or refute our hypothesis. Our hypothesis is that our two alternative operators will outperform existing approaches in terms of throughput, latency, and memory consumption. If the results show lower throughput, worse latency, or higher memory consumption than existing approaches, our hypothesis will be refuted. Conversely, if we achieve higher throughput, lower latency, and lower memory consumption compared to existing approaches, our hypothesis will be supported.

4.5 
For visualization and monitoring of the data and for speculating peformance analysis in real time we intend to use Grafana, which is a multi-platform open source analytics and interactive visualization web application.

### Success Indicators

5.1 
The goal of this project is to develop two window operators that are compatible with multiple window types and measures, and to demonstrate that these operators outperform existing operators that do not implement record buffer and slicing strategies. The expected outcome is to have fully functional and optimized window operators that can effectively handle real-time data streams and provide better performance compared to current approaches.

5.2 
One intermediate milestone for the window measures will be completing the design for the time-based measure, which we will use as a basis for our window measures. We will then move on to designing the count-based measure and extending our window types beyond tumbling windows, such as sliding windows and session windows.

To start, we will focus on constructing our architecture, which involves connecting the Flink window aggregator operator with our record buffer and slicing strategy, and integrating RocksDB as a state backend to process the in-order and out-of-order data streams. Once this initial stage is complete, we will work towards optimizing our approach for different use cases and adjusting parallelism, window size, buffer size, and slicing interval, among other factors. 

5.3 
We will consider the two operators to be completed when they exhibit the same functionality as the default operators in Apache Flink, and can be applied to different window types and measures, as well as handle both in-order and out-of-order streams. While completion does not necessarily equate to success, achieving the desired functionality would be considered a major milestone. Further optimization and testing may be required to achieve a successful implementation.

5.4 
The success of our project will be determined by the achievement of several performance metrics, including higher throughput, lower memory consumption, lower latency, and higher scalability. We will compare the performance of our two window aggregation operators with existing stream slicing approaches and consider the ability of our operators to handle both in-order and out-of-order streams, as well as large volumes of data. Once we have demonstrated superior performance in terms of these metrics, we can declare our project a success.

### Task Assignment:

6.1
Task Breakdown:

We are planning to break down the project into two tasks: developing the alternating window aggregating operators and creating two connectors and integrating RocksDB to serve as a state backend for the stream processing which can be broken down to following tasks:
1.	Construction of a base streaming framework with architecture permeable to the project’s vision (initial setup in flink).
2.	Implementation of Record Buffer for Holistic and Commutative
3.	Implementation of Slicing Operator for Holistic and Commutative
4.	Implementation of the Functions for Commutative and Holistic
5.	Integrate RockDB backend as a way to store states(which involves connectors)
6.	Develop the Slice Manager and Window Manager to handle the operations in accordance with the project vision.
7.	Develop the watermarks and sliding/tumbling window metadata.
8.	Develop the Test cases from the datasets and use them.
9.	Measure the Performance Metrics of the system including throughput and latency.
10.	Brief comparison of our model against flinks default implementation mentioned in the paper.
11.	Analysis and Optimization.(Current Ideas: Extension to Out of order windows and exploiting state compression feature of RocksDB)

6.2
Tasks Dependencies:

Task 1 is platform after which other tasks can be started,

Tasks 2,3,4 can be paralleled 

Task 5 require 2,3,4 testing of state so dependent on previous tasks.

Task 6 controls latency and throughput significantly so it is executed carefully. Though dependent on Tasks till 5. 

Task 7 is dependent on tasks till 6 and Task 8 can be exploited for special cases so dependent on tasks till 6 though both tasks can be parallelly done by members.

Tasks 9 and 10 are interdependent and executed after all tasks

Task 11 is an Extension of our Project.

6.3
Task Distribution:

Aman Ahmed: Has a working understanding of Java, Microservices Though no prior experience in stream systems, has worked in large code bases and has an experience in Backend.

Tasks: 1, 3(commutative), 8, 11

Bhargav Ram Reddy: Has a Working Experience in Stream Systems through Apache spark with Scala. Has not worked extensively with Java but has efficient knowledge of it.

Tasks: 2 (Holistic),3 (Holistic), 10, 11

Nirbhay Malhotra: Has Previous Experience in Java, Flink and Kafka, even with no previous working experience in stream systems is quite knowledgeable in them.

Tasks: 2 (Commutative), 4 (Holistic and Commutative), 11

Rhythm Somaiya: Has a Working Experiece in Java and Hadoop with HDFS Gasn’t worked with stream systems but has a sound knowledge about distributed systems.

Tasks: 6, 7, 10, 11

Yuesi Liu: Has a Working Experience in Java. Though has not worked with stream systems before has a high understanding of Databases and RocksDB. 

Tasks: 5, 6, 9, 11

We are a team of developers with a proficient understanding of Java meanwhile actively expanding our skills in Flink and Kafka. Some of us also have prior experience working with stream systems. We are confident that with our collective expertise, we will leave no stone unturned in accomplishing the project successfully.

