# SparkStreamExamples

## Some pages usefull

links:

>[Spark排错与优化](http://blog.csdn.net/lsshlsw/article/details/49155087)

>[Spark Configuration](http://spark.apache.org/docs/latest/configuration.html)

##notes:

ATO & Sybil detecting

PIT Tagging
  tagging in accountAttr
  logic is in sql in *Tag function

##SSH Trouble Shootting

### Broken Pipe
Error Message: packet_write_wait: Connection to UNKNOWN: Broken pipe

Solution: Some ssh process is broken, need to be killed. Use 'ps awx | grep ssh' to find it.
