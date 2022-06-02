#!/bin/bash
#SBATCH --time 05:00:00
#SBATCH --cpus-per-task=16
#SBATCH --job-name=Amine
#SBATCH --nodes=1
#SBATCH --mem=1000
#SBATCH --output=outfile
#SBATCH --partition=assemblix
# makes the directory if it doesn't exist
mkdir -p output
export path=/local-fs/datasets/refseq_protein
export timings=output/timings.txt
export time=/usr/bin/time
export BLASTDB=/local-fs/datasets/
export blastoutput=output/blastoutput.txt
echo "" > "$timings"
echo "" > "$blastoutput"
for i in {1..16};do
	$time --format %e -a -o "$timings" blastp -query MCRA.faa -db ${path}/refseq_protein -num_threads $i -outfmt 6 >> "$blastoutput";
done
# creates a plot of timings.txt
python assignment3.py
