A script called "assignment1.py" that given 1 starting article's Pubmed ID,
downloads 10 articles referenced by that first article.
It should do this concurrently from PubMed using the Biopython API.

These Pubmed ID can be tested:
# no reference
pmid = 9047029
# more than 10 reference
pmid = "30049270"
# less than 10 reference
pmid = "10737444"

the script should be called in command-line as follows:
	python '.\assignment1.py' -n 10 10737444