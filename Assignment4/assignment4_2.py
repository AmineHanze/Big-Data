import csv
from Bio import SeqIO
from Bio.Seq import Seq
from Bio.SeqRecord import SeqRecord
import sys
import numpy as np
import pandas as pd
import shutil as sh
#find the best n50 and directory
df = pd.read_csv("output/final.csv", header = None)
df.columns = ["dir", "n50"]
df["n50"] =  pd.to_numeric(df["n50"])
i = np.max(list(df["n50"]))
best = df[df["n50"] == i]
best_n50 = best.iloc[0,1]
best_dir = str(best.iloc[0,0])
sentence = f"the best n50 is {best_n50} and best kmer is {best_dir}."
x = []
x.append(sentence)
with open("output/final.csv", "a") as f:
    writer = csv.writer(f)
    writer.writerow(x)
source= r'{}/contigs.fa'.format(best_dir)
destination= r"output/"
sh.copy(source,destination)
del_list = list(df['dir'])
for i in del_list:
    sh.rmtree(str(i))
