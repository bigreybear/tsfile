<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# TsFile Document
<pre>
___________    ___________.__.__          
\__    ___/____\_   _____/|__|  |   ____  
  |    | /  ___/|    __)  |  |  | _/ __ \ 
  |    | \___ \ |     \   |  |  |_\  ___/ 
  |____|/____  >\___  /   |__|____/\___  >  version 1.0.0-zx/research
             \/     \/                 \/  
</pre>

## Preface

For fast comparison, necessary files from Parquet-MR are 
included directly into the submodule `parquet`.

There could be dependency issues due to the rough inclusion, 
it is suggested that you should try building before actually executing any experiments.

The [Parquet-MR](https://github.com/apache/parquet-mr/tree/d8396086b3e3fefc6829f8640917c3bbde0fa9c4) 
are copied from Feb 19 2024.

To validate dependency about Parquet, you can run as follows:
```bash
mvn clean package -pl parquet/parquet-hadoop -am -Dmaven.test.skip=true -Drat.skip=true -Dspotless.check.skip=true -Dcheckstyle.skip=true
```

To build TsFile, you can run:
```bash
mvn clean package -pl tsfile -am -Dmaven.test.skip=true -Drat.skip=true -Dspotless.check.skip=true -Dcheckstyle.skip=true
```

To build a fat jar for reproduce the comparisons, run as follows:
```bash
mvn clean package -pl tsfile -am -Dmaven.test.skip=true -Drat.skip=true -Dspotless.check.skip=true -Dcheckstyle.skip=true -Pjar-with-dependencies
```

## Abstract

TsFile is a columnar storage file format designed for time series data, which supports efficient compression, high throughput of read and write, and compatibility with various frameworks, such as Spark and Flink. It is easy to integrate TsFile into IoT big data processing frameworks.


## Motivation

Time series data is becoming increasingly important in a wide range of applications, including IoT, intelligent control, finance, log analysis, and monitoring systems. 

TsFile is the first existing standard file format for time series data. The industry companies usually write time series data without unification, or use general columnar file format, which makes data collection and processing complicated without a standard. With TsFile, organizations could write data in TsFile inside end devices or gateway, then transfer TsFile to the cloud for unified management in IoTDB and other systems. In this way, we lower the network transmission and the computing resource consumption in the cloud.

TsFile is a specially designed file format rather than a database. Users can open, write, read, and close a TsFile easily like doing operations on a normal file. Besides, more interfaces are available on a TsFile.

TsFile offers several distinctive features and benefits:

* Efficient Storage and Compression: TsFile employs advanced compression techniques to minimize storage requirements, resulting in reduced disk space consumption and improved system efficiency. 

* Flexible Schema and Metadata Management: TsFile allows for directly write data without pre defining the schema, which is flexible for data aquisition. 

* High Query Performance with time range: TsFile has indexed devices, sensors and time dimensions to accelerate query performance, enabling fast filtering and retrieval of time series data. 

* Seamless Integration: TsFile is designed to seamlessly integrate with existing time series databases such as IoTDB, data processing frameworks, such as Spark and Flink. 


 
