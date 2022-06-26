from pyspark.sql import SQLContext
from pyspark import SparkContext
import csv
from pyspark.sql import functions as F
from pyspark.sql.functions import desc, avg, split, countDistinct, explode
#reading file and create dataframe
file = '/data/dataprocessing/interproscan/all_bacilli.tsv'
sc=SparkContext('local[16]')
df = SQLContext(sc).read.csv(file, sep=r'\t', header=False, inferSchema= True)
#rename the column name
new_names = ['Protein_accession', 'MD5', 'Seq_len', 'Analysis',
             'Signature_accession', 'Signature_description',
             'Start', 'Stop', 'Score', 'Status', 'Date', 'InterPro_accession',
             'InterPro_discription', 'GO_annotations', 'Pathways']
df = df.toDF(*new_names)
row = []
#################################################################################
# question #1
A1 = df.filter(df.InterPro_accession!="-").select(countDistinct("InterPro_accession"))
A1_1 = A1.collect()[0][0]
A1_2 = A1._sc._jvm.PythonSQLUtils.explainString(A1._jdf.queryExecution(), 'simple')
row.append([1, A1_1, A1_2])
# #################################################################################
# question #2
A2 = df.filter(df.InterPro_accession!="-").groupBy('Protein_accession').agg(F.count('InterPro_accession')).agg(F.mean('count(InterPro_accession)'))
A2_1 = A2.collect()[0][0]
A2_2 = A2._sc._jvm.PythonSQLUtils.explainString(A2._jdf.queryExecution(), 'simple')
row.append([2, A2_1, A2_2])
# #################################################################################
# question #3
A3 = df.filter(df.GO_annotations!="-").withColumn("go", explode(split(df.GO_annotations, "[|]"))).groupBy("go").count().sort(desc("count"))
A3_1 = A3.collect()[0][0]
A3_2 = A3._sc._jvm.PythonSQLUtils.explainString(A3._jdf.queryExecution(), 'simple')
row.append([3, A3_1, A3_2])
#################################################################################
# question #4
A4 = df.withColumn('Result', (df['Stop'] - df['Start'])).agg(avg('Result'))
A4_1 = A4.collect()[0][0]
A4_2 = A4._sc._jvm.PythonSQLUtils.explainString(A4._jdf.queryExecution(), 'simple')
row.append([4, A4_1, A4_2])
#################################################################################
# question #5
A5 = df.filter(df.InterPro_accession!="-").groupBy("InterPro_accession").count().sort(desc("count"))
A5_1 = A5.rdd.flatMap(lambda x: x).collect()[0:20:2]
A5_2 = A5._sc._jvm.PythonSQLUtils.explainString(A5._jdf.queryExecution(), 'simple')
row.append([5, A5_1, A5_2])
# #################################################################################
# question #6
A6 = df.filter(df.InterPro_accession!="-").withColumn('Result', ((df['Stop'] - df['Start'] ) / df['Seq_len']) >= 0.9).sort(desc("Result")).select("InterPro_accession")
a = A6.rdd.flatMap(lambda x: x).collect()
A6_1 = []
for i in a:
    if i in A6_1:
        continue
    else:
        A6_1.append(i)
        if len(A6_1) == 10:
            break
A6_2 = A6._sc._jvm.PythonSQLUtils.explainString(A6._jdf.queryExecution(), 'simple')
row.append([6, A6_1, A6_2])

# A6 = df.filter(df.InterPro_accession!="-").where(((df['Stop'] - df['Start'] ) / df['Seq_len']) >= 0.9).sort(desc("Result")).select("InterPro_accession")
# a = A6.rdd.flatMap(lambda x: x).collect()
# A6_1 = []
# for i in a:
#     if i in A6_1:
#         continue
#     else:
#         A6_1.append(i)
#         if len(A6_1) == 10:
#             break
# A6_2 = A6._sc._jvm.PythonSQLUtils.explainString(A6._jdf.queryExecution(), 'simple')
# row.append([6, A6_1, A6_2])
#################################################################################
# question 7
A7 = df.filter(df.InterPro_discription!="-").withColumn('NameArray', explode(split(df.InterPro_discription,",\\s* | \\s*"))).groupby('NameArray').count().sort(desc("count"))
A7_1 = A7.rdd.flatMap(lambda x: x).collect()[0:20:2]
A7_2 =A7._sc._jvm.PythonSQLUtils.explainString(A7._jdf.queryExecution(), 'simple')
row.append([7, A7_1, A7_2])
#################################################################################
# question 8
A8_1 = A7.rdd.flatMap(lambda x: x).collect()[-2:-21:-2]
A8_2 = A7._sc._jvm.PythonSQLUtils.explainString(A7._jdf.queryExecution(), 'simple')
row.append([8, A8_1, A8_2])
################################################################################
# question 9
# # A9 = df.filter(df.InterPro_discription!="-").filter(df.InterPro_accession!="-").withColumn('Result', (( df['Stop'] - df['Start'] ) / df['Seq_len'] )>= 0.9).withColumn('NameArray', explode(split(df.InterPro_discription,",\\s* | \\s*"))).groupby('NameArray').count().sort(desc("count"))
A9 = df.filter(df.InterPro_discription!="-").filter(df.InterPro_accession!="-").withColumn('NameArray', explode(split(df.InterPro_discription,",\\s* | \\s*"))).where(( (df['Stop'] - df['Start'] ) / df['Seq_len'])> 0.9).groupby('NameArray').count().sort(desc("count"))
A9_1 = A9.rdd.flatMap(lambda x: x).collect()[0:20:2]
A9_2 = A9._sc._jvm.PythonSQLUtils.explainString(A9._jdf.queryExecution(), 'simple')
row.append([9, A9_1, A9_2])
#################################################################################
# question 10
A10 = df.filter(df.InterPro_accession !="-").groupBy('Protein_accession','Seq_len').count()
A10_1 = A10.corr('Seq_len','count')**2
A10_2 = A10._sc._jvm.PythonSQLUtils.explainString(A10._jdf.queryExecution(), 'simple')
row.append([10, A10_1, A10_2])
#################################################################################
#write in csv file
with open("output/output.csv", "a") as f:
    writer = csv.writer(f)
    writer.writerows(row)
print("finished successfully")
