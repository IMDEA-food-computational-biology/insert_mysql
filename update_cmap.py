import inspect
import re
import shutil
import os
import tarfile
from time import time
import h5py
import numpy as np
import os
import sys
import pandas as pd
import inspect
import sys
sys.path.append(".")
from utils import *

def tar_untar_file_list(files,direc, func, tar_kwargs = {}, func_kwargs = {}):

    """
    Given list of tared file in direc,
    """
    ret = {}
    try:
        
        for f in files:

            tarf = tarfile.open(f,**tar_kwargs)
            tarf.extractall(path = direc)
            tarf.close()


            filename =  os.listdir(direc)[0]
            filename =  name + "/" + filename
            ret[f] = func(filename,**func_kwargs)
            shutil.rmtree(filename)
            
    finally:  
        
        shutil.rmtree(direc)

    return ret

def tar_untar_direc(direc,regexp,func,**kwargs):
    
    """
    pass a function on a bunch of tared files whose names 
    follow some regex
    
    untar files in a helper directory
    do the function on untared files/directories, save the return in a variable
    remove the untared files
    remove helper directory
    return the variable
    """
    files = [direc + x for x in  os.listdir(direc)]
    files = re.findall(regexp,"\n".join(files))
    
    #something needs to be done about this
    #the point is to untar in an empty directory
    try:
       
        name = ".taredFiles_"+str(time())
        os.makedirs(name)
        
    except FileExistsError:
        
        pass
    
    #extract which arguments from kwargs belong to tarfile.open and which
    #to user function
    tar_args = [k for k, v in inspect.signature(tarfile.open).parameters.items()]
    tar_dict = {k: kwargs.pop(k) for k in dict(kwargs) if k in tar_args}
    

    func_args = [k for k, v in inspect.signature(func).parameters.items()]
    func_dict = {k: kwargs.pop(k) for k in dict(kwargs) if k in func_args}
    
    ret = tar_untar_file_list(files,name, func, tar_dict, func_dict)
        
    return ret


def summarize(matrix,to_add, char_sep = ":", summary = np.median):

    add = np.core.defchararray.add

    for i,adding in enumerate(to_add):

        if i == 0:

            nodes = adding

        else:

            nodes = add(add(nodes, char_sep), adding)

    nodes = np.unique(nodes)

    list_of_dicts = [dict() for _ in range(len(to_add))]

    for i,adding in enumerate(to_add):

        list_of_dicts[i] = {x: adding == x for x in np.unique(adding)}

    vals = np.zeros(len(nodes))
    starter_and = np.ones(len(nodes), bool)

    for i,node in enumerate(nodes):


        splitted = node.split(char_sep)
        for j, (dict_,key) in enumerate(zip(list_of_dicts,splitted)):

            if j == 0:

                bools = dict_[key]

            else:

                bools = np.logical_and(bools, dict_[key])

        idx = np.where(bools)[0]
        vals[i] = summary(matrix[idx])

    return nodes,vals

def extract_interactions(root_direc,file,groupby,sep,m = "a+",*args,**kwargs):

    """
    Directory: directory of cmap responses, obtained by untaring the,
    file: Which data file to use
    destination: where to write the interactions
    groupby: what variables to group the different cmap samples, can be a tuple if more than one
    sep: separator for interactions file
    m: file mode, such as writing or appending
    """
    file = root_direc + "/"+ "matrices/gutc/" + file
    results = h5py.File(file, "r")
    matrix = np.array(results["0"]["DATA"]["0"]["matrix"])[0]
    groupby_ = [0 for _ in range(len(groupby))]
    name = np.array(results["0"]["META"]["COL"]["id"]).astype("O")[0]
    name = name.decode("utf-8", errors = "replace")

    for i,group in enumerate(groupby):

        groupby_[i] = np.array(results["0"]["META"]["ROW"][group]).astype(str)

    #extract which arguments from kwargs belong to open and which
    #to summarize
    #unnames args go to open
    open_args = [k for k, v in inspect.signature(open).parameters.items()]
    open_dict = {k: kwargs.pop(k) for k in dict(kwargs) if k in open_args}


    summarize_args = [k for k, v in inspect.signature(summarize).parameters.items()]
    summarize_dict = {k: kwargs.pop(k) for k in dict(kwargs) if k in summarize_args}

    nodes,vals = summarize(matrix, groupby_,**summarize_dict)
    
    return name, nodes, vals


def get_cmap_nodes_to_ids(cursor):

    query = """select * from cmap"""
    cursor.execute(query)
    result = cursor.fetchall()
    column_names = cursor.column_names
    cmap_table = pd.DataFrame(result, columns=column_names)
    cmap_nodes_to_ids = {}

    for i,r in cmap_table.iterrows():

        node = r["compound"] + ":" + r["cell_line"] + ":" + r["pert_type"]
        cmap_node_id = r["cmap_node_id"]
        cmap_nodes_to_ids[node] = cmap_node_id

    return cmap_nodes_to_ids

def insert_mysqlCMAP(node_id, cmap_nodes, cmap_nodes_to_ids, vals,n):

    insert = """INSERT INTO cmap_foodrugs (`node_id`,`cmap_node_id`,`tau`) VALUES"""
    for i, (cmap_node, tau) in enumerate(zip(cmap_nodes, vals)):

        cmap_node_id = cmap_nodes_to_ids[cmap_node]
        body = f"""({node_id}, {cmap_node_id}, {tau})"""
        statement = form_statement(insert, body, n, i)
        yield statement

def insert_to_mysql(connection, cursor, node_id, cmap_nodes, cmap_nodes_to_ids, vals,n):

    generator = insert_mysqlCMAP(node_id, cmap_nodes, cmap_nodes_to_ids, vals,n)
    insert_from_generator(connection, cursor, generator, n) #append, because for each query the code is ran


def write_to_mysqlFile(fname, node_id, cmap_nodes, cmap_nodes_to_ids, vals,n):

    generator = insert_mysqlCMAP(node_id, cmap_nodes, cmap_nodes_to_ids, vals,n)
    write_from_generator(fname, generator, "a+") #append, because for each query the code is ran

def write_to_file(fname, name, nodes, vals, sep = "\t", mode = "a+"):

    with open(fname, mode) as f:

        for node, val in zip(nodes, vals):

            f.write(name + sep + node + sep + str(val) + "\n")

def main(root_direc, node_id, insert, connection, cursor, cmap_nodes_to_ids, n, mysql_file_name, network_file_name):

    name,nodes,vals = extract_interactions(root_direc, file = "ps_pert_cell.gctx",groupby = ("pert_iname", "cell_id", "pert_type"),sep = "\t",m = "a+")

    if insert:

        insert_to_mysql(connection, cursor, node_id, nodes, cmap_nodes_to_ids, vals,n)

    else:

        write_to_mysqlFile(mysql_file_name, node_id, nodes, cmap_nodes_to_ids, vals,n)

    write_to_file(network_file_name, name, nodes, vals, sep = "\t", mode = "a+")





if __name__ == "__main__":

    import os
    from stat import S_ISFIFO
    import argparse
    from time import time
    from mysql.connector import connect, Error
    from getpass import getpass
    
    parser = argparse.ArgumentParser(description='Update cmap_foodrugs table in cmap and writes to a network file. \
            Updates table from cmap query response for cell line level resolution.\
            Pass it a directory containing such files with CMAP_data flag or pipe a list of files')
    parser.add_argument('--insert', dest='insert', action='store_true',
                        help='Use this flag to insert data directly into FooDrugs_v2.')

    parser.add_argument('--mysqlFile', dest='insert', action='store_false',
                        help='Use this flag to create a mysql file with the commands neccessary to insert the data.')

    parser.add_argument('--file', dest='file', type= str,default = None,
                        help='File name for the mysql file if using --mysqlFile. Defaults to TM_update_time where time is time in seconds since...')

    parser.add_argument('--CMAP_data', dest='CMAP_data', type= str,
                        help='Directory containing cmap data. FIle name should be node_id followed by __ and then the rest does not matter. \
                                Files should be .tar.gz. Can pipe a list of files, in which case this flag is ignored')

    parser.add_argument('--n-records-per-batch', dest='n', type= int, default = 6000,
                        help='number of records per batch insert. Default 6000')
    
    parser.add_argument('--network_file_name', dest='network_file_name', type= str,default = None,required = True,
                        help='File name for the interaction network')
    
    parser.set_defaults(insert = True)
    args = parser.parse_args()

    try:

        #if being piped to, input function recieves the name of files as 
        connection = connect(host = "localhost", 
                         user = getpass("Enter username: "),
                        password = getpass("Enter password: "),
                        database = getpass("Enter database: "))

        cursor = connection.cursor()

        cmap_nodes_to_ids = get_cmap_nodes_to_ids(cursor)
        file = args.file

        if file is None:

                file = f"mysql_dump_cmap_{time()}.sql"

        main_args = {"insert": args.insert, "connection": connection, 
                    "cursor": cursor, "cmap_nodes_to_ids": cmap_nodes_to_ids, 
                    "n": args.n, "mysql_file_name": file, 
                    "network_file_name": args.network_file_name}

        #make empty directory to untar cmap files
        if S_ISFIFO(os.fstat(0).st_mode):

            print("Reading from pipe")

            for cmap_file in sys.stdin:

                name = ".taredFiles_"+str(time())
                os.makedirs(name)
                cmap_file_name = cmap_file.split("/")[-1]
                node_id = int(cmap_file_name.split("_")[0])
                main_args["node_id"] = node_id
                t1 = time()
                tar_untar_file_list([cmap_file[:-1]],name, main, tar_kwargs = {}, func_kwargs = main_args)
                t2 = time()
                print(f"{cmap_file[:-1]} done, took {t2 - t1:.3f} seconds")
        else:
            
            files = [args.CMAP_data + "/" + f for f in os.listdir(args.CMAP_data)]
            for cmap_file in files:

                name = ".taredFiles_"+str(time())
                os.makedirs(name)
                cmap_file_name = cmap_file.split("/")[-1]
                node_id = int(cmap_file_name.split("_")[0])
                main_args["node_id"] = node_id
                t1 = time()
                tar_untar_file_list([cmap_file],name, main, tar_kwargs = {}, func_kwargs = main_args)
                t2 = time()
                print(f"{cmap_file} done, took {t2 - t1:.3f} seconds")

    finally:

        cursor.close()
        connection.close()
