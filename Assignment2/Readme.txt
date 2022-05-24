this script analyze the XML of each of the references further to extract all the authors of the article.
It saves the authors in a Python tuple and use the Pickle module to save it to the disk as output/PUBMED_ID.
authors.pickle where PUBMEDID is of course the pubmed ID of the article in question.

how to run:
is server and client separately:
python .\assignment2.py -a 5 -n 4 --port 4235 --host nuc411 -s 30049270
python .\assignment2.py -a 5 -n 4 --port 4235 --host nuc411 -c 30049270