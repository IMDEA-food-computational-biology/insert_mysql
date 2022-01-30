import sys
sys.path.append(".")
from utils import *
from collections import defaultdict
import sys

def get_FDI_pairs(fname, cursor):

    """
    Given a file named `fname` consisting of
    tab separated link, TM_pipeline outputs columns and a 
    mysql cursor

    returns:

        links_to_ids: dict of link, new mysql ids
        ids_to_outputs: dict of links, list of all the outputs with that link
    """
    links_to_ids = {}
    ids_to_outputs = defaultdict(list)

    #get corresponding ID for the links (texts table)
    curr = get_next_ID(cursor, "texts", "texts_ID")

    with open(fname) as f:
    
        for i,line in enumerate(f, start = 1):

            try:

                link, output = line[:-1].split(" ", 1)
                if link not in links_to_ids:

                    links_to_ids[link] = curr
                    curr += 1

                ids_to_outputs[links_to_ids[link]].append(output)
            
            except ValueError:

                sys.stderr.write(f"Wrongly formatted line in {fname} at line {i}\n")

    
    return links_to_ids, ids_to_outputs

def get_texts(fname):

    """
    Given a file named `fname` consisting of
    tab separated link, text and a 
    mysql cursor

    returns:

        links_to_texts: dict of link, texts

    WARNING: links with more than one text will be discarded
    """
    links_to_text = defaultdict(list)
    
    with open(fname) as f:
    
        for i,line in enumerate(f, start = 1):
            
            try:
                
                link, text = line[:-1].split("\t", 1)
                links_to_text[link].append(text)
                
            except ValueError:
                
                sys.stderr.write(f"Wrongly formatted line in {fname} at line {i}\n")

    #returns only links that have one text
    n_text_per_link = {l:len(t) for (l,t) in links_to_text.items()}
    links_to_text = {l:t[0] for (l,t) in links_to_text.items() if n_text_per_link[l] == 1}
    
    for (k,v) in n_text_per_link.items():

        if v > 1:

            sys.stderr.write(f"Link {k} in {fname} associated with {v} texts, skipping, only one allowed\n")

    return links_to_text

def yield_inserts_texts(links_to_ids, links_to_text,n):

    """
    yields inserts to write/insert in mysql file/connection of batch size n
    first yields texts inserts (link,text,id) then yields TM_interaction queries
    """

    #yield texts queries
    line_n = 0
    insert =  """INSERT INTO texts (`texts_ID`, `link`, `document`) values"""
    final_idx = len(links_to_text) - 1

    for l,text in links_to_text.items():

        #links to ids comes from TM_pipeline output, if a text had no interaction, 
        #it will not be in the dictionary
        try:

            ID = links_to_ids[l]
            body = f"""({ID}, "{l}", "{text}")"""
            statement = form_statement(insert,body,n,line_n)
            line_n += 1
            yield statement

        except KeyError:

            pass

def yield_inserts_FDI(links_to_ids, links_to_text, ids_to_outputs, n, cursor):

    ids = {links_to_ids[l] for l in links_to_text if l in links_to_ids}
    curr_TM_interactions = get_next_ID(cursor, "TM_interactions", "TM_interactions_ID")
    insert = """INSERT INTO TM_interactions(TM_interactions_ID, texts_ID, start_index, end_index, food, drug) VALUES"""
    line_n = 0

    for ID, outputs in ids_to_outputs.items():

        if ID in ids:

            try:

                for output in outputs:

                    start, end, food, food_norm, drug, drug_norm = output.split(":")
                    start = int(start)
                    end = int(end)

                    body=f"""({curr_TM_interactions}, {ID},{start}, {end}, "{food}", "{drug}")"""
                    statement = form_statement(insert,body,n,line_n)
                    curr_TM_interactions += 1
                    line_n += 1
                    yield statement

            except ValueError:

                pass

def insert_to_mysql(connection, cursor, text_file, FDI_file,n):

    links_to_ids, ids_to_outputs = get_FDI_pairs(FDI_file, cursor)
    links_to_text = get_texts(text_file)
    
    insert_from_generator(connection, cursor, yield_inserts_texts(links_to_ids, links_to_text,n), n)
    insert_from_generator(connection, cursor, yield_inserts_FDI(links_to_ids, links_to_text, ids_to_outputs, n, cursor), n)
    
        


def write_to_mysql_file(fname, cursor, text_file, FDI_file,n):

    links_to_ids, ids_to_outputs = get_FDI_pairs(FDI_file, cursor)
    links_to_text = get_texts(text_file)

    write_from_generator(fname, yield_inserts_texts(links_to_ids, links_to_text,n), "w+")
    write_from_generator(fname, yield_inserts_FDI(links_to_ids, links_to_text, ids_to_outputs, n, cursor), "a+")


if __name__ == "__main__":


    import argparse
    from time import time
    from mysql.connector import connect, Error
    from getpass import getpass
    
    parser = argparse.ArgumentParser(description='Update text miining table in FooDrugs_v2.')
    parser.add_argument('--insert', dest='insert', action='store_true',
                        help='Use this flag to insert data directly into FooDrugs_v2.')

    parser.add_argument('--mysqlFile', dest='insert', action='store_false',
                        help='Use this flag to create a mysql file with the commands neccessary to insert the data.')

    parser.add_argument('--file', dest='file', type= str,default = None,
                        help='File name for the mysql file if using --mysqlFile. Defaults to TM_update_time where time is time in seconds since...')

    parser.add_argument('--links_texts', dest='links_texts', type= str, required = True,
                        help='Tab separated file with first column a url, second a text')

    parser.add_argument('--TM_outputs', dest='TM_outputs', type= str, required = True,
                        help='File with output of TM_pipeline.py where id is a url found in --links_text file')

    parser.add_argument('--n-records-per-batch', dest='n', type= int, default = 6000,
                        help='number of records per batch insert. Default 6000')

    parser.set_defaults(insert = True)
    args = parser.parse_args()

    connection = connect(host = "localhost", 
                     user = input("Enter username: "),
                    password = getpass("Enter password: "),
                    database = input("Enter database: "))

    cursor = connection.cursor()

    n = args.n
    TM_outputs = args.TM_outputs
    links_texts = args.links_texts
    file = args.file

    if args.insert:

        insert_to_mysql(connection, cursor, links_texts, TM_outputs,n)

    else:

        if file is None:

            file = f"mysql_dump_TM_{time()}.sql"

        write_to_mysql_file(file, cursor, links_texts, TM_outputs,n) 
