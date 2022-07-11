"""
----------------------------------------------------------------------
Subject : develop scikit-learn machine learning models to
predict the function of the proteins on InterPROscan dataset
version 1.0
Module : programming 3 (big data)
Lecturer: Martijn Herber
Author : Amine Jalali
-----------------------------------------------------------------------
"""
import os
import pyspark
from pyspark.sql import SQLContext
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
from pyspark.sql.types import IntegerType
from pyspark.ml.classification import LogisticRegression, NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler

conf = pyspark.SparkConf().setAll([('spark.executor.memory', '128g'),
                                   ('spark.master', 'local[16]'),
                                   ('spark.driver.memory', '128g')])
sc = pyspark.SparkContext(conf=conf)
sc.getConf().getAll()

file = '/data/dataprocessing/interproscan/all_bacilli.tsv'

df = SQLContext(sc).read.csv(file, sep=r'\t', header=False, inferSchema= True)
new_names = ['Protein_accession', 'MD5', 'Seq_len', 'Analysis',
             'Signature_accession', 'Signature_description',
             'Start', 'Stop', 'Score', 'Status', 'Date', 'InterPro_annotations',
             'InterPro_discription', 'GO_annotations', 'Pathways']
df = df.toDF(*new_names)
# drop useless columns
df = df.drop("MD5", "Analysis", "Signature_accession", "Signature_description",\
"Score", "Status", "Date", "InterPro_discription", "GO_annotations", "Pathways")
# filter out lines don't contain annotation
df = df.filter(df.InterPro_annotations!="-")
df = df.withColumn('size', (((df['Stop'] - df['Start']) / df.Seq_len) * 100))
# create a dataframe for large features
large_df = df.where (df.size >= 90)
windowpro = Window.partitionBy("Protein_accession").orderBy(col("size").desc())
largdf = large_df.withColumn("row",row_number().over(windowpro)).filter(col("row") == 1).drop("row")
largdff = largdf.drop("Seq_len", "Start", "Stop", "size")
# list of relevant proteins to large annotations
ap = largdf.select("Protein_accession").collect()
all_protein = [i[0] for i in ap]
# find small annotations of relevant proteins to large annotations
small_df = df.filter(df.Protein_accession.isin(all_protein))
small_df = small_df.filter(df.size < 90)
# transpose small_df to set InterPro_annotations to columns
pivotDF = small_df.groupBy("Protein_accession").pivot("InterPro_annotations").count()
pivotDF = pivotDF.fillna(value=0)
# left join of pivotDF and largdff
df_left = pivotDF.join(largdff, on=['Protein_accession'], how='left')
# find numerical columns to change the type
all_columns = df_left.columns
numericCols = [i for i in all_columns if (i != "Protein_accession") & (i != "InterPro_annotations")]
# change the type of all numerical columns from long to integer 
df_left = df_left.select(df_left.Protein_accession,(*(col(c).cast(IntegerType()).alias(c) \
for c in numericCols)), df_left.InterPro_annotations)
# Combine all numerical columns into a single feature vector
assembler = VectorAssembler(inputCols=numericCols, outputCol="features")
ML_df = assembler.transform(df_left)
# encode a string column of labels to a column of label indices 
label_stringIdx = StringIndexer(inputCol = 'InterPro_annotations', outputCol = 'labelIndex')
ML_df = label_stringIdx.fit(ML_df).transform(ML_df)
mlf = ML_df.select("features", "labelIndex")
(train, test) = mlf.randomSplit(weights=[0.3, 0.7])
# applying NaiveBayes model
nb = NaiveBayes(modelType="multinomial", featuresCol = 'features', labelCol = 'labelIndex')
nbmodel = nb.fit(train)
predictions = nbmodel.transform(test)
evaluator = MulticlassClassificationEvaluator(labelCol="labelIndex",\
    predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
result_nb = "Naive Bayes accuracy = " + str(accuracy)
print(result_nb)
# create output directory
dir = "output"
if not os.path.exists(dir):
    os.mkdir(dir)
# writing in the result file
with open('output/result.txt', 'a', encoding="utf-8") as f:
    f.write(result_nb)
# applying Logistic Regression
lr = LogisticRegression(featuresCol = 'features', labelCol = 'labelIndex')
lrModel = lr.fit(train)
predictions = lrModel.transform(test)
evaluator = MulticlassClassificationEvaluator(labelCol="labelIndex", predictionCol="prediction")
accuracy = evaluator.evaluate(predictions)
result_lr = "\nLogistic Regression accuracy = " + str(accuracy)
print(result_lr)
# writing in the result file
with open('output/result.txt', 'a', encoding="utf-8") as f:
    f.write(result_lr)
# saving dataframe for implementing more machine learning models
ML_df.write.option("header",True).csv("final_NB.csv")
