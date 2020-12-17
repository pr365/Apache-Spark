from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("SpendByCustomer")
sc = SparkContext(conf = conf)

def extractCustomerPricePairs(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))

input = sc.textFile("file:///sparkcourse/customer-orders.csv")
mappedInput = input.map(extractCustomerPricePairs)
totalByCustomer = mappedInput.reduceByKey(lambda x, y: x + y)
sort = totalByCustomer.map(lambda x:(x[1],x[0]))
sq = sort.sortByKey()
sq1 = sq.map(lambda x:(x[1],x[0]))

results = sq1.collect();
for result in results:
    print(result)
