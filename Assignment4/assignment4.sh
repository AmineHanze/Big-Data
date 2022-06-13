file1=/data/dataprocessing/MinIONData/MG5267/MG5267_TGACCA_L008_R1_001_BC24EVACXX.filt.fastq
file2=/data/dataprocessing/MinIONData/MG5267/MG5267_TGACCA_L008_R2_001_BC24EVACXX.filt.fastq
mkdir -p output
seq 21 2 31 | parallel mkdir -p {}
seq 21 2 31 | parallel -j16 velveth {} {} -fastq -longPaired -separate $file1 $file2
seq 21 2 31 | parallel -j16 velvetg {}
#seq 21 2 23 | parallel -j16 "python assignment4.py {}" >> final_result.csv
seq 21 2 31 | parallel -j16 "python assignment4_1.py "
python assignment4_2.py
