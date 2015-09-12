# zkcopy

Tool for fast copying ZooKeeper data between different clusters. 
Optimized for copying big volumes of data over WAN.

## Build

Requires [apache maven 3](https://maven.apache.org/).

```
$ mvn clean install
```

## Usage

```
java -jar target/zkcopy-*-jar-with-dependencies.jar
    -Dsource="server:port/path" 
    -Ddestination="server:port/path" 
    -Dthreads=10
    -DremoveDeprecatedNodes=true
```

* `source` - set source cluster address and root node to be copied;
* `destination` - set target cluster address and root node location where to
  copy data;
* `threads` - specify number of parallel workers to copy data. If latency if
  high, then increasing this value might significantly improve copying speed.
* `DremoveDeprecatedNodes` - set it to `true` for removing nodes that are 
  present on `target` but missing on `source`;
