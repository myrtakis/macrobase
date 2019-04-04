Benchmark for anomaly detection algorithms and implementations of several anomaly detection algorithms (iforest, MCOD, LOF, ...).

# Running benchmark

1. Build the whole MacroBase project using Maven `package` phase from your IDE (IntelliJ Idea, ...) or [install Maven](https://maven.apache.org/install.html) and use this command: 
```
cd ..
mvn package
```
2. Run `java -jar target/benchmark.jar <parameters>`

   Example:
  
   `java -jar target/benchmark.jar -b data/outlier/iforest_shuttle_config.yaml --data-dir .. --so output`
