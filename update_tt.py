import sys
sys.path.append(".")
from utils import *
import os
from time import time

def isfloat(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

def groupby_nodes(topTableFiles, gmtFiles):

    group_by_nodes = {}
    for f in topTableFiles:

        node = f.split("/")[-1].split("__")[0]
        group_by_nodes[node] = [f,None,None]

    for f in gmtFiles:

        try:

            node = f.split("/")[-1].split("__")[0]
            if f[-6:-4] == "UP":

                group_by_nodes[node][1] = f

            elif f[-6:-4] == "DN":
                group_by_nodes[node][2] = f

        except KeyError:

            sys.stdout.write(f"GMT file {f} has no corresponding topTable file\n")

    return group_by_nodes

def insert_tt(topTableFiles, gmtFiles, n):

    group_by_nodes = groupby_nodes(topTableFiles, gmtFiles)
    insert  = """insert into topTable (`entrez_Id`,`logFC`, `AveExpr`, `moderated_t`, `P_value`, `adjusted_P_value`, `B`, `node_id`, `geneSet`) values"""
    line_n = 0

    for node, (tt,UP,DN) in group_by_nodes.items():
        
        t1 = time()
        UP_set = set()
        DN_set = set()
        
        if UP is not None:
            
            with open(UP) as f:
                
                genes = f.readline()[:-1].split("\t")[2:]
                UP_set.update(genes)
                    
        if DN is not None:
            
            with open(DN) as f:
                
                genes = f.readline()[:-1].split("\t")[2:]
                DN_set.update(genes)
                    
        with open(tt) as f:
            
            for i,line in enumerate(f):
                
                #ignore header
                if i == 0:
                    
                    continue
                    
                fields = line[:-1].replace('"',"").split(" ")
                if len(fields) != 8:

                    sys.stdout.write(f"Line {i} in top table file {tt} doesnt have 8 fields, skipping \n")
                    continue

                probeset, entrezIDs, logFC, AveExpr, t, P_Value, adj_P_Val, B = fields

                #if it has no entrezID or expression value/t value, skip
                if not entrezIDs or entrezIDs == "NA" or not isfloat(entrezIDs) or AveExpr == "NA" or t == "NA":
                    
                    continue
                    
                if entrezIDs in UP_set:
                    
                    geneSet = f"'UP'"
                    
                elif entrezIDs in DN_set:
                    
                    geneSet = f"'DN'"
                    
                else:
                    
                    geneSet = "NULL"
                    
                
                body = f"""({entrezIDs}, {logFC}, {AveExpr}, {t}, {P_Value}, {adj_P_Val}, {B}, {node}, {geneSet})"""
                statement = form_statement(insert, body, n, line_n)                
                line_n += 1
                yield statement

        t2 = time()
        print(f"Processing ({tt}, {UP}, {DN}) done!, took {t2 -t1:.3f} seconds")

def main(topTableFiles, gmtFiles,insert, mysqlFile, n, connection, cursor):

    generator = insert_tt(topTableFiles, gmtFiles, n)

    if insert:

        insert_from_generator(connection, cursor, generator, n)

    else:

        write_from_generator(mysqlFile, generator, "w+") 


if __name__ == "__main__":

    import os
    from stat import S_ISFIFO
    import argparse
    from time import time
    from mysql.connector import connect, Error
    from getpass import getpass
    
    parser = argparse.ArgumentParser(description='Update topTable table in FooDrugs_v2 . \
            Pass it a directory containing top table files and another one containing gnmt files or pipe a list of files')
    parser.add_argument('--insert', dest='insert', action='store_true',
                        help='Use this flag to insert data directly into FooDrugs_v2.')

    parser.add_argument('--mysqlFile', dest='insert', action='store_false',
                        help='Use this flag to create a mysql file with the commands neccessary to insert the data.')

    parser.add_argument('--file', dest='file', type= str,default = None,
                        help='File name for the mysql file if using --mysqlFile. Defaults to TM_update_time where time is time in seconds since...')

    parser.add_argument('--topTable', dest='topTable', type= str,
                        help='Directory containing cmap data. FIle name should be node_id followed by __ and then the rest does not matter. \
                                Files should be .tar.gz. Can pipe a list of files, in which case this flag is ignored')
    
    parser.add_argument('--gmtFiles', dest='gmtFiles', type= str,default = None,
                        help='File name for the interaction network')

    parser.add_argument('--n-records-per-batch', dest='n', type= int, default = 6000,
                        help='number of records per batch insert. Default 6000')
    
    
    parser.set_defaults(insert = True)
    args = parser.parse_args()

    try:
        connection = connect(host = "localhost", 
                         user = getpass("Enter username: ", stream = sys.stdout),
                        password = getpass("Enter password: "),
                        database = getpass("Enter database: "))

        cursor = connection.cursor()
        file = args.file

        if file is None:

                file = f"mysql_dump_cmap_{time()}.sql"


        #make empty directory to untar cmap files
        if S_ISFIFO(os.fstat(0).st_mode):

            print("Reading from pipe")

            files = [f[:-1] for f in sys.stdin.readlines()]
            topTableFiles = [f for f in files if "top_table" in f]
            gmtFiles = [f for f in files if f.endswith(".gmt")]

        else:
            
            topTableFiles = [args.topTable + "/" + f for f in os.listdir(args.topTable)]
            gmtFiles = [args.gmtFiles + "/" + f for f in os.listdir(args.gmtFiles)]
        
        main(topTableFiles, gmtFiles, args.insert, file, args.n, connection, cursor)

    finally:

        cursor.close()
        connection.close()

