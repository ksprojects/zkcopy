# zkcopy

Tool for fast copying ZooKeeper data between different clusters.
Optimized for copying big volumes of data over WAN.

## Build

Requires [apache maven 3](https://maven.apache.org/).

```bash
mvn clean install
```

## Usage

```bash
java -Dsource="server:port/path" \
     -Ddestination="server:port/path" \
     -Dthreads=10 \
     -DremoveDeprecatedNodes=true \
     -jar target/zkcopy-*-jar-with-dependencies.jar
```

If using [docker](https://hub.docker.com/r/kshchepanovskyi/zkcopy/) then:

```bash
docker pull kshchepanovskyi/zkcopy
docker run --rm -it kshchepanovskyi/zkcopy \
    -Dsource="server:port/path" \
    -Ddestination="server:port/path" \
    -Dthreads=10 \
    -DremoveDeprecatedNodes=true
```

* `source` - set source cluster address and root node to be copied
* `destination` - set target cluster address and root node location where to
  copy data
* `threads` - specify number of parallel workers. If latency is
  high, then increasing this number might significantly improve performance
* `removeDeprecatedNodes` - set it to `true` to remove nodes that are
  present on `destination` but missing on `source`
