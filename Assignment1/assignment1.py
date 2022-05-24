from Bio import Entrez
import multiprocessing as mp
import argparse as ap
from pathlib import Path
import os


Entrez.api_key = "d4cea981fb2c06210b961c26c207cac00c08"
Entrez.email = "amine.jalali@gmail.com"

def find_refrences(pmid, n):
    """ This function return the first n references of pmid
    """
    record = Entrez.read(Entrez.elink(dbfrom="pubmed", id=pmid, LinkName="pubmed_pmc_refs"))
    link_list = record[0]["LinkSetDb"]
    # if there is reference
    if link_list:
        references = [f'{link["Id"]}' for link in record[0]["LinkSetDb"][0]["Link"]]
    # if there is not any references
    else:
        references = []
    return references[:n]

def download_files(reference):
    """ This function download the reference and write it down
    in the reference.xml in output directory
    """
    handle = Entrez.efetch(db="pmc", id=reference, rettype="XML", retmode="text")
    with open(f'output/{reference}.xml', 'wb') as file:
        file.write(handle.read())

if __name__ == "__main__":
    argparser = ap.ArgumentParser(description="Script that downloads (default) 10 articles referenced by the given PubMed ID concurrently.")
    argparser.add_argument("-n", action="store",
                           dest="n", required=False, type=int,
                           help="Number of references to download concurrently.")
    argparser.add_argument("pubmed_id", action="store", type=str, nargs=1, help="Pubmed ID of the article to harvest for references to download.")
    args = argparser.parse_args()
    # in the number of refrences entered
    if args.n:
        n = args.n
    else:
        n = 10
    print("Getting: ", args.pubmed_id)
    # get the first n references
    references = find_refrences(args.pubmed_id, n)
    cpus = mp.cpu_count()
    # check if article has any references
    if not os.path.isdir("output"):
        os.makedirs("output")
    if references:
        with mp.Pool(cpus) as pool:
            # parallelize the execution of function across multiple references
            pool.map(download_files, references)
        print("References are downloaded successfully!")
    #if there is no reference:
    else:
        print("There is no reference!")


