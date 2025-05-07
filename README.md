# Big Data for E-commerce

## Big data analysis of e-commerce platforms（电商平台大数据分析）


During the practical training, I carried out the public opinion processing of telecommunication data. Subsequently, I attempted to conduct the big data analysis of e-commerce data by myself. I wrote a JAR package using Java. Then, I uploaded it to the Hadoop cluster and implemented four parts, namely data production, data collection, consumption and storage, data analysis, and data display. The uploaded content of this project is the JAR package I wrote. The versions of Hadoop, Hive, Flume, and Kafka can be found in my pom.xml, and I won't list them here.




As China's e-commerce industry booms, traditional platforms struggle with sluggish sales growth and inefficient data processing. With data storage expanding from GB to PB levels, and business and consumer needs evolving, data analysis is now key. However, traditional analytics architectures fall short in handling large-scale, real-time data. Big data frameworks like Hadoop present new opportunities. This system's design and implementation act as an e-commerce platform's big data analysis project.


本项目分为四个部分，分别为数据生产、数据采集/消费/存储、数据分析以及数据展示，以下是具体介绍：

```mermaid
flowchart TB
    classDef process fill:#E5F6FF,stroke:#73A6FF,stroke-width:2px,color:#000000;
    A([Data Production]):::process -->|"Data format example: 2022064074, Yang Min, phone, 2, 79.99, 159.98, 2024-07-10 13:44:46"| B([Data Collection/Consumption/Storage]):::process
    B -->|"Online data --- Flume --- Kafka --- HBase (HDFS)"| C([Data Analysis]):::process
    C --> D([Data Display]):::process
    subgraph Objectives
        style Objectives fill:#ffffff,stroke:#000000,stroke-width:1px;
        O1(Statistics of the quantity of goods purchased by users per day, and the total amount of goods purchased per day.)
        O2(Statistics of the purchase frequency of users per month, and the cumulative consumption amount of users per month.)
        O3(Statistics of daily and monthly sales volume of goods.)
    end
    C -.-> O1
    C -.-> O2
    C -.-> O3
    subgraph ResultDisplay
        style ResultDisplay fill:#ffffff,stroke:#000000,stroke-width:1px;
        R(Display the results queried for a certain person's certain product according to different dimensions based on business requirements on the Web page.)
    end
    D --> ResultDisplay
