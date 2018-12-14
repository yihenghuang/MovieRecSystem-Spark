
sc = SparkContext()

training_data = sc.textFile('s3://n.xie-emr/input_proj')
 
user_counts=training_data.map(lambda x:(x.split(“,”)[0],1)).reduceByKey(lambda v1,v2:v1+v2)

testing_data = sc.textFile('s3://n.xie-emr/input_test')

rating_counts=testing_data.map(lambda x:(x.split(“,”)[1],1)).reduceByKey(lambda v1,v2:v1+v2)

final=training_data.map(lambda x:(x.split(“,”)[1],x.split(“,”)[0])).join(rating_counts).values().reduceByKey(lambda v1,v2:v1+v2).join(user_counts).mapValues(lambda v:v[0]/v[1])


final.values().saveAsTextFile('s3://n.xie-emr/overlap')

sc.stop()

