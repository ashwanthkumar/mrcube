[![Build Status](https://snap-ci.com/ashwanthkumar/mrcube/branch/master/build_image)](https://snap-ci.com/ashwanthkumar/mrcube/branch/master)

# Scalding MRCube

Scalding CUBE operators

Naive `cubify` and `rollup` methods on richPipe.

For each Input Tuple

- cubify generates 2^n tuples, where n is the number of fields we are cubing on
- rollup generates n+1 tuples, where n is the number of fields we are rolling up on

### Dev
```bash
$ git clone https://github.com/ashwanthkumar/mrcube.git
$ sbt test
```

### Dependencies
For Maven,
```xml
<dependency>
  <groupId>in.ashwanthkumar</groupId>
  <artifactId>mrcube_2.10</artifactId>
  <version>0.12.0</version>
</dependency>
```

For SBT,
```sbt
libraryDependencies += "in.ashwanthkumar" %% "mrcube" % "0.12.0"
```

### Cubify

If the input tuple is  ``("ipod", "miami", "2012", "200000")`` the output generated from the job is

```
("ipod", "miami", "2012", "1", "200000.0")
("ipod", "miami", "null", "1", "200000.0")
("ipod", "null", "null", "1", "200000.0")
("ipod", "null", "2012", "1", "200000.0")
("null", "null", "2012", "1", "200000.0")
("null", "miami", "null", "1", "200000.0")
("null", "miami", "2012", "1", "200000.0")
("null", "null", "null", "1", "200000.0")
```

Instead of "null" you can pass in another custom string to cubify.

```scala
import in.ashwanthkumar.mrcube._

class CubifyJob(args: Args) extends Job(args) {

  Csv(args("input"), fields = ('product, 'location, 'year, 'sales))
    .read
    .cubify(('product, 'location, 'year))
    .groupBy('product, 'location, 'year) { _.size('size).sum[Int]('sales) }
    .write(Csv(args("output")))
}
```


### Rollup

If the input tuple is  ``("ipod", "miami", "2012", "200000")`` the output generated from the job is

```
("ipod", "miami", "2012", "1", "200000.0")
("null", "miami", "2012", "1", "200000.0")
("null", "null", "2012", "1", "200000.0")
("null", "null", "null", "1", "200000.0")
```

Similarly instead of "null" you can pass in another custom string to rollup.

```scala
import in.ashwanthkumar.mrcube._

class RollupJob(args: Args) extends Job(args) {
  Csv(args("input"), fields = ('product, 'location, 'year, 'sales))
    .read
    .rollup(('product, 'location, 'year))
    .groupBy('product, 'location, 'year) { _.size('size).sum[Int]('sales) }
    .write(Csv(args("output")))

}
```

### References

1. [Distributed Cube Materialization on Holistic Measures](http://arnab.org/files/mrcube.pdf) by [Dr. Arnam Nandi](http://arnab.org/) et. al
2. CUBE Operator in Pig - [PIG 2167](https://issues.apache.org/jira/browse/PIG-2167)

### License
Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
