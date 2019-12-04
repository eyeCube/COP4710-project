'''
    CDA 4710
    Databases project
    Jacob Wharton, Andre Guiraud, Samuel Silva
    Due: 6 December 2019


what is h_mean, a_mean, a_median?
what should the input function REPL look like?
Can we connect o_code to major?

    more fxns:
    
    
    advanced fxn:
    location and major -> info about jobs relating to major in your area
'''

# mysql connector:
#   pip install -U setuptools
#   pip install -U wheel
#   pip install mysql-connector-python-rf
import mysql.connector
import sys


# constants
DATABASE_NAME = "jobs"
    # database column names
JOB_ID = "o_code"
JOB_NAME = "o_name"
#


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


# advanced
def convert_location(loc):
    # translate "Miami, FL" into
    # "Miami-Fort Lauderdale-West Palm Beach, FL"
    data = loc.split(", ")
    sql = "SELECT l_name FROM location WHERE l_name LIKE '%{}%, {}'".format(
        data[0], data[1])
    cursor.execute(sql)
    return cursor.fetchone()[0] #return only the first item in the row

def get_rows_from_major_location(cursor, major, area_code):
    sql = "SELECT * FROM jobs WHERE major = '{}' AND l_code = '{}'".format(
        major,area_code
        )
    cursor.execute(sql)
    return cursor.fetchall()
#

def get_location_code(cursor, lname)
    sql = "SELECT l_code FROM location WHERE l_name = '{}'".format(lname)
    cursor.execute(sql)
    return cursor.fetchone()[0] #return only the first item in the row

def get_location_codes(cursor):
    sql = "SELECT DISTINCT l_code FROM location"
    cursor.execute(sql)
    return cursor.fetchall()

def get_occupation_codes(cursor):
    sql = "SELECT DISTINCT o_code FROM jobs"
    cursor.execute(sql)
    return cursor.fetchall()

def insert_args(cursor, *args):
    '''        
        add a record to the database, using standard arguments
            i.e. insert(cursor, "john", 12039,...)
    '''
    print("insert_args")
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
# end def

def insert_kwargs(cursor, **kwargs):
    '''
        add a record to the database, using keyword arguments
            i.e. insert(cursor, name="john", emp_id=12039,...)
    '''
    print("insert_kwargs")
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
# end def

def delete_where(cursor, condition):
    sql = "DELETE FROM {db} WHERE {con}".format(db="salary", con=condition)
    cursor.execute(sql)
    sql = "DELETE FROM {db} WHERE {con}".format(db="occupation", con=condition)
    cursor.execute(sql)
def delete(cursor, itemID): # delete by ID
    print("delete")
    delete_where(cursor, "{} = '{}'".format(JOB_ID, itemID))
def delete_by_name(cursor, name):
    delete_where(cursor, "{} = '{}'".format(JOB_NAME, name))

def select(cursor, condition):
    sql = "SELECT FROM {db} WHERE {con}".format(
        db="occupation", con=condition )
    cursor.execute(sql)
    return cursor.fetchall()
def select_by_name(cursor, name):
    return select(cursor, "{} = '{}'".format(JOB_NAME, name))

def update_entry(cursor, job_id, **kwargs):
    print("update")
    
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


def menu():
    print("~~~~~~~~~~~~~~~~~~~~")
    print("    Commands:")
    print("        i : insert new item into database")
    print("        d : delete item by ID")
    print("        D : delete item satisfying condition")
    print("        u : update")
    print("        s : select")
    print("        S : select item satisfying condition")
    print("        q : quit")

def printColumns():
    print('''
Columns:
    AREA        : string
    AREA_NAME   : string
    OCC_CODE    : string -> occupation type
    OCC_TITLE   : string
    H_MEAN      : float
    A_MEAN      : int
    A_MEDIAN    : int
''')

def main():
    print(len(sys.argv))
    assert(len(sys.argv) >= 3)

    host = sys.argv[1]
    user = sys.argv[2]
    pw = sys.argv[3]
    dbname = sys.argv[4] if len(sys.argv) >= 5 else DATABASE_NAME
    cnx, cursor = establish_connection(host, user, pw, dbname)
    
    programIsRunning = True
    while (programIsRunning):
        menu()
        opt=input()
        
        if opt=='i':
            # get location
            while(True):
                print("Enter a valid location e.g. 'Tallahassee, FL':")
                loc=input()
                area_code = get_location_code(loc)
                print(area_code)
                for item in get_location_codes():
                    acode = item[0] # each row is a tuple
                    if acode==area_code:
                        break
                print("Invalid response. Try again.")
            # end while

            # get occupation code
            # how to handle this? How do we want insert to work?
            while(True):
                print("Enter a valid occupation code e.g. '11-1011':")
                o_code=input()
                
                # is this how this should be??
                print(o_code)
                if item in get_occupation_codes():
                    ocode = item[0] # each row is a tuple
                    if ocode==o_code:
                        break
                print("Invalid response. Try again.")
            # end while
            
            print("Enter the occupation title:")
            o_title = input()
            print("Enter the occupation h_mean:")
            h_mean = input()
            print("Enter the occupation a_mean:")
            a_mean = input()
            print("Enter the occupation a_median:")
            a_median = input()
            
            insert_args(cursor, (
                area_code, area_name, o_code, o_title, h_mean, a_mean, a_median,
                )
            )
            cnx.commit()
        # end if
            
        elif opt=='d':
            print("Enter the OCC_CODE of the item to delete:")
            inp=input()
            delete(cursor, inp)
            cnx.commit()
            
        elif opt=='D':
            print("Enter the condition on which to delete:")
            inp=input()
            delete_where(cursor, inp)
            cnx.commit()
            
        elif opt=='u':
            print("Enter the ID of the item to update:")
            item=input()
            printColumns()
            print("Enter the new data for the row, delimited by ', ':")
            argstr=input()
            arglist = argstr.split(", ")
            update(cursor, item, arglist)
            cnx.commit()
            
        elif opt=='s':
            print("Enter the name of the occupation to search for:")
            inp=input()
            results = select_by_name(cursor, inp)
            print(results)
            
        elif opt=='S':
            print("Enter the condition on which to search:")
            inp=input()
            results = select(cursor, inp)
            print(results)
            
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
    '''

'''

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
'''
