import os


def truncate_utf8_chars(filename, count, ignore_newlines=True):
    """
    Truncates last `count` characters of a text file encoded in UTF-8.
    :param filename: The path to the text file to read
    :param count: Number of UTF-8 characters to remove from the end of the file
    :param ignore_newlines: Set to true, if the newline character at the end of the file should be ignored
    
    taken from: https://stackoverflow.com/questions/18857352/remove-very-last-character-in-file
    """
    with open(filename, 'rb+') as f:
        last_char = None

        size = os.fstat(f.fileno()).st_size

        offset = 1
        chars = 0
        while offset <= size:
            f.seek(-offset, os.SEEK_END)
            b = ord(f.read(1))

            if ignore_newlines:
                if b == 0x0D or b == 0x0A:
                    offset += 1
                    continue

            if b & 0b10000000 == 0 or b & 0b11000000 == 0b11000000:
                # This is the first byte of a UTF8 character
                chars += 1
                if chars == count:
                    # When `count` number of characters have been found, move current position back
                    # with one byte (to include the byte just checked) and truncate the file
                    f.seek(-1, os.SEEK_CUR)
                    f.truncate()
                    return
            offset += 1

def get_next_ID(cursor, table, id_field):

    """
    get max id of table with name id_field plus 1,
    0 if null
    """


    query = f"""select max({id_field}) from {table}"""
    cursor.execute(query)
    result = cursor.fetchall()[0][0]

    if result is None:

        return 0

    else:

        return result + 1

def form_statement(insert,body,n,line_n):

    """
    Return the sql statement to write dependending
    on batch size and current position in batch

    if start of batch, add `insert` string (some type of insert statement)
    if at end of batch, add a ;, else write a ,
    """


    if line_n%n == 0:

        to_add = insert

    else:

        to_add = ""

    if line_n%n == (n -1):

        to_end = ";"

    else:

        to_end = ","

    return f"{to_add}{body}{to_end}\n"

def write_from_generator(fname,generator, type_ = "w+"):


    with open(fname, type_) as f:

        for statement in generator:

            f.write(statement)

    #the last insert should include a ;
    #we cannot know from the yield if it does, as it 
    #only produces a ; if last_line%n==(n-1), and a ,
    #otherwise. Remove last two chars (newline and , or ;)
    #and add a ; and newline to be sure
    truncate_utf8_chars(fname, 2, ignore_newlines=False)
    
    with open(fname, "a+") as f:

        f.write(";\n")

def insert_from_generator(connection, cursor, generator,n):

    #adding to list the strings then joining is faster
    #than string concatenation

    for i,statement in enumerate(generator):
        
        if i % n == 0:

            inserts = [None for _ in range(n)]
        
        insert_index = i%n
        inserts[insert_index] = statement

        if i%n == (n -1):

            query = "".join(inserts)
            cursor.execute(query)

    if i%n != (n -1):

        inserts = inserts[:inserts.index(None)]
        inserts[-1] = inserts[-1][:-2] + ";\n"
        query = "".join(inserts)
        cursor.execute(query)
        
    connection.commit()
