'''
    CDA 4710
    Databases project
    Jacob Wharton, Andre Guiraud, Samuel Silva
    Due: 6 December 2019
'''

# mysql connector:
#   pip install -U setuptools
#   pip install -U wheel
#   pip install mysql-connector-python-rf
import mysql.connector


DATABASE_NAME = "jobs"


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


def delete_where(cursor, condition):
    database = DATABASE_NAME
    sql = "DELETE FROM {db} WHERE {con}".format(database, condition)
    cursor.execute(sql)
    
def delete_by_name(cursor, name):
    delete_where(cursor, "name = {}".format(name))

def update_entry(cursor, job_id, **kwargs):
    database = DATABASE_NAME
    condition = "job_id = {}".format(job_id) # what's the column name?

    # create the set field
    setfield = ""
    for k,v in kwargs.items():
        setfield += "{} = {}, ".format(k, v)
    setfield=setfield[:-2] # remove final ", "
    
    sql = "UPDATE {db} SET {kw} WHERE {con}".format(
        db=database, kw=setfield, con=condition
        )
    cursor.execute(sql)
    return cursor.fetchall()
# end def


# main function -- testing
def main():
    cnx, cursor = establish_connection(host, user, pw, database)
    
    
    cnx.commit() # commit changes to the database
    cursor.close() # finally, make sure we close stuff
    cnx.close()

if __name__=="__main__":
    insert(None, "INSERT INTO jobs (par1, par2, par3, par4) VALUES (1, 2, 3, 4)")
    
##    main()



# EXAMPLES BELOW


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
