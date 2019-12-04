'''
    CDA 4710
    Databases project
    Jacob Wharton, Andre Guiraud, Samuel Silva
    Due: 6 December 2019
    
    more fxns:
    
    
    advanced fxn:
    location and major -> info about jobs relating to major in your area
    get average hourly wage for all jobs of a given major in your area
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
        host='localhost',
        user='root',
        passwd='Lexmark1!',
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




def get_location_code(cursor, lname): #updated: test again
    sql = "SELECT l_code FROM location WHERE l_name = '{}'".format(
        convert_location(lname) )
    cursor.execute(sql)
    results = cursor.fetchone()
    if len(results):
        return results[0] #return only the first item in the row
    else:
        return None

def get_location_codes(cursor):
    sql = "SELECT DISTINCT l_code FROM location"
    cursor.execute(sql)
    return cursor.fetchall()

def get_occupation_code(cursor, o_name): # TEST
    sql = "SELECT DISTINCT o_code FROM majors WHERE m_name LIKE '%{}%'".format(
        o_name )
    cursor.execute(sql)
    results = cursor.fetchone()
    if len(results):
        return results[0] #return only the first item in the row
    else:
        return None

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
        db="occupation", v=valuestrs
        )
    cursor.execute(statement, tuple(_values))
# end def

def insert_occupation(cursor, o_code, o_name):
    sql = "INSERT INTO occupation (o_code, o_name) VALUES ('{o_code}', '{o_name}')".format(
        o_code, o_name
        )
    cursor.executs(sql)
def insert_salary(cursor, h_mean, a_mean, a_median, o_code, l_code):
    sql = '''INSERT INTO occupation (h_mean, a_mean, a_median, o_code, l_code)
VALUES ({h_mean}, {a_mean}, {a_median}, '{o_code}', {l_code}'''.format(
        h_mean, a_mean, a_median, o_code, l_code
        )
    cursor.executs(sql)

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
def delete_salary(cursor, condition):
    sql = "DELETE FROM {db} WHERE {con}".format(db="salary", con=condition)
    cursor.execute(sql)
def delete(cursor, itemID): # delete by ID
    print("delete")
    delete_where(cursor, "{} = '{}'".format(JOB_ID, itemID))
def delete_by_name(cursor, name):
    delete_where(cursor, "{} = '{}'".format(JOB_NAME, name))

def select(cursor, condition, what):
    sql = "SELECT {w} FROM {db} WHERE {con}".format(
        w=what, db="occupation", con=condition )
    cursor.execute(sql)
    return cursor.fetchall()
def select_by_name(cursor, name):
##    "{} = '{}'"
    return select(cursor, "{} LIKE '%{}%'".format(JOB_NAME, name), "o_name")
def select_o_code(cursor, name):
    # get o_code from o_name
##    "{} = '{}'"
    return select(cursor, "o_name LIKE '%{}%'".format(name), "o_code")

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
# end def

#add this little function to the definition
def search():

    print(" What do you want to base your search on?")
    print("~~~~~~~~~~~~~~~~~~~~")
    print("    Commands:")
    print("        m : major")
    print("        a : area")
    print("        i : major and area")

    option=input()
    if option == 'm':
        print("Enter your major's name")
        m = input()
        query = ("SELECT m_name, l_name, h_mean, a_mean, a_median FROM expected_salary, major, location"
                         "WHERE m_name == %s")

        cursor.execute(query, (m,))
        return cursor.fetchall()

    elif option == 'a':
        print("Enter your state's name")
        a = input() 
        query = ("SELECT m_name, l_name, h_mean, a_mean, a_median FROM expected_salary, major, location"
                         "WHERE l_name == %s")

        cursor.execute(query, (a,))
        return cursor.fetchall()
    elif option == 'i':
        print("Enter your major's name")
        m = input()
        print("Enter your area's name")
        a = input()
        query = ("SELECT m_name, l_name, h_mean, a_mean, a_median FROM expected_salary, major, location"
                         "WHERE m_name == %s AND l_name == %s")

        cursor.execute(query, (m,a,))
        return cursor.fetchall()

    else :
        print("Invalid option")
        return search()
#end def search



# advanced
def convert_location(cursor, loc):
    # translate "Miami, FL" into
    # "Miami-Fort Lauderdale-West Palm Beach, FL"
    data = loc.split(", ")
    sql = "SELECT l_name FROM location WHERE l_name LIKE '%{}%, {}'".format(
        data[0], data[1])
    cursor.execute(sql)
    results = cursor.fetchone()
    if (len(results)):
        return results[0] #return only the first item in the row
    else:
        return None
#get average hourly wage for all jobs of a given major in a given area
def get_hourly_avg(cursor, majorName, locName):
    o_code = get_occupation_code(cursor, majorName)[:2] # only care about first two digits in the code
    l_name = convert_location(cursor, locName) # get a valid location
    if o_code == None:
        return None
    if l_name == None:
        return None
    sql = '''SELECT AVG(h_mean) FROM jobs, location
    WHERE jobs.o_code LIKE '{}%' AND location.l_name = '{}'
    '''.format(o_code, l_name)
    cursor.execute(sql)
    return cursor.fetchall()
# redundant ?
def get_rows_from_major_location(cursor, major, area_code):
    # should it be this way? We need to use JOINS
    '''SELECT * FROM jobs JOIN location ON location.l_code=jobs.area
    WHERE major = '{}' AND l_code = '{}'
    '''
    sql = "SELECT * FROM jobs WHERE major = '{}' AND l_code = '{}'".format(
        major,area_code
        )
    cursor.execute(sql)
    return cursor.fetchall()
# end advanced



def menu():
    print("Enter a command to begin.")
    print("~~~~~~~~~~~~~~~~~~~~")
    print("    Commands:")
##    print("        i : insert new item into database")
    print("        is: insert new salary into database")
    print("        io: insert new occupation into database")
    print("        d : delete occupation by ID")
    print("        ds: delete salary by ID and loc") 
    print("        u : update")
    print("        s : select")
    print("        ss: search for a job by major or location")
    print("        o : get occupation code matching job title")
    print("        l : get location code matching location name")
    print("        I : insert new item into database (each column)")
    print("        D : delete item satisfying condition")
    print("        S : select item satisfying condition")
    print("        a : get hourly average")
    print("        q : quit")

def printColumns():
    # TODO: change these to the appropriate names
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
        
        if opt=='io':
            print("Enter the o_code")
            inp=input()
            print("Enter the o_name")
            inp1=input()
            insert_occupation(cursor, inp, inp1)
            cnx.commit()
        
        if opt=='is':
            #h_mean, a_mean, a_median, o_code, l_code
            print("Enter the hourly mean")
            inp=input()
            print("Enter the annual mean")
            inp1=input()
            print("Enter the annual median")
            inp2=input()
            print("Enter the occupation code")
            inp3=input()
            print("Enter the location code")
            inp4=input()
            insert_salary(cursor, inp, inp1, inp2, inp3, inp4)
            cnx.commit()
        
        elif opt=='i':
            # get location
            while(True):
                print("Enter a valid location e.g. 'Tallahassee, FL':")
                loc=input()
                area_code = get_location_code(cursor, loc)
                print(area_code)
                if area_code != None:
##                    for item in get_location_codes(cursor):
##                        acode = item[0] # each row is a tuple
##                        if acode==area_code:
##                            break
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
                if o_code != None:
##                    for item in get_occupation_codes():
##                        ocode = item[0] # each row is a tuple
##                        if ocode[:2]==o_code[:2]: # only care about first two numbers.
##                            break
                    break
                print("Invalid response. Try again.")
            # end while
            
            print("Enter the occupation title:")
            o_title = input()
            print("Enter the occupation hourly mean:")
            h_mean = input()
            print("Enter the occupation annual mean:")
            a_mean = input()
            print("Enter the occupation annul median:")
            a_median = input()
            
            insert_args(cursor, (a_code, o_code, o_title, h_mean, a_mean, a_median,
                )
            )
            cnx.commit()
        # end if
            
        elif opt=='I':
            print("Enter the new row data, delimited by ', ':")
            inp=input()
            inps = inp.split(", ")
            insert_args(cursor, inps)
            cnx.commit()
            
        elif opt=='d':
            print("Enter the o_code of the item to delete:")
            inp=input()
            delete(cursor, inp)
            cnx.commit()
	
        elif opt=='ds':
            print("Enter the o_code of the salary to delete:")
            inp=input()
            print("Enter the l_code of the salary to delete:")
            inp1=input()
            delete_salary(cursor, "o_code = '{}' AND l_code = '{}'".format(
                inp, inp1) )
            cnx.commit()
            
        elif opt=='D':
            print("Enter the condition on which to delete:")
            inp=input()
            delete_where(cursor, inp)
            cnx.commit()
            
        elif opt=='u': # TODO: update update fxn
            print("Enter the o_code of the item to update:")
            item=input()
            printColumns()
            print("Enter the new data for the row, delimited by ', ':")
            argstr=input()
            arglist = argstr.split(", ")
            update_entry(cursor, item, arglist)
            cnx.commit()

        # select by name
        elif opt=='s':
            print("Enter the name of the occupation to search for:")
            inp=input()
            results = select_by_name(cursor, inp)
            print(results)

        # search by major / area
        elif opt=='ss':
            print(search())

        # search avg
        elif opt=='a':
            print("Enter the major name:")
            inp=input()
            print("Enter the location name:")
            inp1=input()
            results = get_hourly_avg(cursor, inp, inp1)
            if results:
                print(results)
            else:
                print("No results."

        # search by condition
        elif opt=='S':
            print("Enter the condition on which to search:")
            inp=input()
            results = select(cursor, inp)
            print(results)

        # get occupation code from occupation name
	elif opt=='o':
           print("Enter the name of occupation:")
           inp=input()
           results = select_o_code(cursor, inp)
           print(results)

	elif opt=='l':
	   print("Enter a valid location e.g. 'Tallahassee, FL':")
	   loc=input()
           area_code = get_location_code(cursor, loc)
           print(area_code)
            
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
