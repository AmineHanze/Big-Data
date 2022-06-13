import csv
from Bio import SeqIO
from Bio.Seq import Seq
from Bio.SeqRecord import SeqRecord
import sys
import numpy as np
import pandas as pd

### create csv file
#dirs = list(sys.argv[1:])
#kmer_list = []
#row = []
#for dir in dirs:
#    with open(dir + "/contigs.fa") as handle:
#        for record in SeqIO.parse(handle, "fasta"):
#            sequence = record.seq
#            kmer_list.append(len(sequence))
#    kmer_list.sort(reverse=True)
#    num_list = kmer_list
#    average = (sum(num_list))/2
#    sum_num = 0
 #   for kmer in kmer_list:
 #       sum_num = sum_num + kmer
#        if sum_num >= average:
 #           n50 = kmer
 #           break
 #   row.append([dir, n50])
#with open("output/final.csv", "w") as f:
#    writer = csv.writer(f)
 #   writer.writerows(row)
#--------
dirs = list(sys.argv[1:])
kmer_list = []
row = []
for dir in dirs:
    with open(dir + "/contigs.fa") as handle:
        for record in SeqIO.parse(handle, "fasta"):
            sequence = record.seq
            kmer_list.append(len(sequence))
    kmer_list.sort(reverse=True)
    num_list = kmer_list
    average = (sum(num_list))/2
    sum_num = 0
    for kmer in kmer_list:
        sum_num = sum_num + kmer
        if sum_num >= average:
            n50 = kmer
            #sequence = kmer[1]
            break
    row.append([dir, n50])
#np.savetxt("output/final.csv", row)
with open("output/final.csv", "a") as f:
    writer = csv.writer(f)
    writer.writerows(row)
