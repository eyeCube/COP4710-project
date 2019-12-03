'''
    CDA 4710
    Databases project
    Jacob Wharton, Andre ..., Sam ...
    Due: 6 December 2019
'''

# mysql connector:
#   pip install -U setuptools
#   pip install -U wheel
#   pip install mysql-connector-python-rf
import mysql.connector


# constants
DATABASE_NAME = "Florida_OES"
    # database column names
JOB_ID = "OCC_CODE"
JOB_NAME = "OCC_TITLE"
#

'''
columns:
    AREA        string
    AREA_NAME   string
    OCC_CODE    string -> occupation type
    OCC_TITLE   string
    H_MEAN      float
    A_MEAN      int
    A_MEDIAN    int
'''


# get connection, cursor objects
def establish_connection(host, user, pw, database=None):
    if database==None:
        database = DATABASE_NAME
    cnx = mysql.connector.connect(
        host=host,
        user=user,
        passwd=pw,
        database=database
        )
    cursor = cnx.cursor()
    return (cnx, cursor,)

# cq : create query functions
def cq_insert():
    pass

# parse and perform aggragate queries
def parse_multiquery(inp: str):
    pass

def choose_query_from_input(inp: str):
    _inp = inp.lower()
    
    if _inp.find("insert") != -1:
        return insert(inp)
    
    if _inp.find("delete") != -1:
        return delete(inp)
    
    if _inp.find("update") != -1:
        return update(inp)
    
    if _inp.find("select") != -1:
        return select(inp)


def insert_args(cursor, *args):
    '''        
        add a record to the database, using standard arguments
            i.e. insert(cursor, "john", 12039,...)
    '''
    valuestrs = []
    _values = []
    for arg in args:
        valuestrs.append("%s, ")
        _values.append(arg)
    valuestrs=valuestrs[:-2] # remove last ", "
        
    statement = "INSERT INTO {db} VALUES ({v})".format(
        db=DATABASE_NAME, v=valuestrs
        )
    cursor.execute(statement, tuple(_values))
    return cursor.fetchall() # fetch all rows from the query result
        
def insert_kwargs(cursor, **kwargs):
    '''
        add a record to the database, using keyword arguments
            i.e. insert(cursor, name="john", emp_id=12039,...)
    '''
    params = []
    valuestrs = []
    _values = []
    for k,v in kwargs.items():
        #ensure this is a key in database...
        #...
        params.append("{}, ".format(k))
        valuestrs.append("%s, ")
        _values.append(v)
    params=params[:-2] # remove last ", "
    valuestrs=valuestrs[:-2] # remove last ", "
    
    statement = "INSERT INTO {db} ({p}) VALUES ({v})".format(
        db=DATABASE_NAME, p=params, v=valuestrs
        )
    cursor.execute(statement, tuple(_values))
    return cursor.fetchall() # fetch all rows from the query result
# end def

def delete_where(cursor, condition):
    sql = "DELETE FROM {db} WHERE {con}".format(
        db=database, con=condition )
    cursor.execute(sql)
def delete(cursor, itemID): # delete by ID
    delete_where(cursor, "{} = {}".format(JOB_ID, itemID))
def delete_by_name(cursor, name):
    delete_where(cursor, "{} = {}".format(JOB_NAME, name))

def search_by_name():
    pass

def update_entry(cursor, job_id, **kwargs):
    database = DATABASE_NAME
    condition = "job_id = {}".format(job_id) # what's the column name?

    # create the set field
    setfield = ""
    for k,v in kwargs.items():
        # SQL uses 1 for true, 0 for false
        if v is True:
            v = 1
        elif v is False:
            v = 0
        setfield += "{} = {}, ".format(k, v)
    setfield=setfield[:-2] # remove final ", "
    
    sql = "UPDATE {db} SET {kw} WHERE {con}".format(
        db=database, kw=setfield, con=condition
        )
    cursor.execute(sql)
    return cursor.fetchall()
# end def


# end def
# INSERT INTO TABLE (...) VALUES (...)
def _insert(cursor, statement):
    values_index = statement.find("VALUES (")
    paren_index = statement.find("(")

    # parameters given
    # (two tuples provided in SQL string)
    if paren_index < values_index:
        
        # parameters
        endparen_index = statement.find(")")
        params = statement[paren_index+1 : endparen_index]
        params = params.split(", ")
        # confirm that all parameters == column names in database
        #...
        # get parameters as an SQL string again
        paramstr = "("
        for param in params:
            paramstr += param + ", "
        paramstr = "{})".format(paramstr[:-2])
        
        # sql statement
        sql1 = statement[:paren_index-1]
        sql2 = statement[endparen_index+2 : values_index+6]
        
        # statement
        statement = "{} {} {}".format(sql1, paramstr, sql2)
        
        # data
        endparen_index = statement.find(")", endparen_index + 1)
        datastr = statement[values_index+8 : endparen_index]
        data = datastr.split(", ")
        
        print(statement)
        print(sql1)
        print(params)
        print(paramstr)
        print(sql2)
        print(data)
    
    # no parameters given
    # (only one tuple provided in SQL string)
    else:
        
        # parameters
        params = cursor.column_names
    
        # sql statement
        sql = statement[:values_index+6]
        
        # data
        endparen_index = statement.find(")")
        datastr = statement[values_index+8 : endparen_index]
        data = datastr.split(", ")
        
##        statement = ...
        
        print(params)
        print(sql)
        print(data)

    # TODO: test with database
##    cursor.execute(statement, data)
##    records = cursor.fetchall() # fetch all rows from the query result
##    return records
# end def

def menu():
    print("~~~~~~~~~~~~~~~~~~~~")
    print("    Commands:")
    print("        i : insert new item into database")
    print("        d : delete item by ID")
    print("        D : delete item satisfying condition")
    print("        u : update")
    print("        q : quit")

def main(*args):
    assert(len(args) > 4)

    host = args[1]
    user = args[2]
    pw = args[3]
    dbname = args[4] if len(args) >= 5 else DATABASE_NAME
    cnx, cursor = establish_connection(host, user, pw, dbname)
    
    programIsRunning = True
    while (programIsRunning):
        menu()
        opt=input()
        if opt=='i':
            inp=input()
            if inp.find("=") != -1:
                insert_kwargs(inp)
            else:
                insert_args(inp)
            cnx.commit()
        elif opt=='d':
            print("Enter the OCC_CODE of the item to delete:")
            inp=input()
            delete(inp)
            cnx.commit()
        elif opt=='D':
            print("Enter the condition on which to delete:")
            inp=input()
            delete_where(inp)
            cnx.commit()
        elif opt=='u':
            print("Enter the ID of the item to update:")
            item=input()
            print("Enter the new data for the row:")
            arglist=input()
            update(item, arglist)
            cnx.commit()
        elif opt=='q':
            programIsRunning=False
    # end while
    
    cnx.commit()
    cursor.close()
    cnx.close()
# end main
            

if __name__=="__main__":
    main()
    


'''
# select
select_statement = "SELECT * FROM jobs WHERE major_id = %s"
cursor.execute(select_statement, ID_COMPUTERSCIENCE)
records = cursor.fetchall() # fetch all rows from the query result


cnx.commit() # commit changes to the database
cursor.close() # finally, make sure we close stuff
cnx.close() 
'''



insert_statement = ( # SQL statement
        "INSERT INTO jobs"
        "VALUES (%s, %s, %s, %s, %s, %s)"
        )
    data = ( # tuple of data to be inserted
        "Junior Systems Analyser",
        ID_COMPUTERSCIENCE,
        "Leon",
        42000,
        20000,
        74000,
        )
    cursor.execute(insert_statement, data)
    records = cursor.fetchall() # fetch all rows from the query result
    
