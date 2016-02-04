Sys.setenv(SPARK_HOME="/Users/panda/git-store/spark")
system(paste(Sys.getenv("SPARK_HOME"), "R/install-dev.sh", sep = "/"))
.libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))
library(SparkR)
#source(paste(Sys.getenv("SPARK_HOME"), "R/pkg/R/sparkR.R", sep = "/"))
#source.files <- paste(Sys.getenv("SPARK_HOME"), "R/pkg/R", sep = "/")
#file.sources = list.files(source.files, recursive = TRUE,
#                          pattern="*\\.R$", full.names=TRUE, 
#                          ignore.case=TRUE, include.dirs = TRUE)
#file.partial.order <- c(
#  'schema.R', 'generics.R', 'jobj.R', 'RDD.R', 'pairRDD.R',
#  'column.R', 'group.R', 'DataFrame.R', 'SQLContext.R', 'backend.R',
#  'broadcast.R', 'client.R', 'context.R', 'deserialize.R',
#  'functions.R', 'mllib.R', 'serialize.R', 'sparkR.R', 'stats.R',
#  'types.R', 'utils.R')

#source.files <- paste(Sys.getenv("SPARK_HOME"), "R/pkg/R", file.partial.order, sep = "/")

#sapply(file.sources, source)

sc <- sparkR.init(master="local")
sqlContext <- sparkRSQL.init(sc)
x <- createDataFrame(sqlContext, iris)
x$Species <- NULL
model <- kmeans(x, 2)
sparkR.stop()
