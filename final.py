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
import prettytable


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
        convert_location(cursor, lname) )
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
    sql = "SELECT DISTINCT m_id FROM major WHERE m_name LIKE '%{}%'".format(
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
    sql = "INSERT INTO occupation (o_code, o_name) VALUES ('{}', '{}')".format(
        o_code, o_name
        )
    cursor.execute(sql)
def insert_salary(cursor, h_mean, a_mean, a_median, o_code, l_code):
    sql = "INSERT INTO salary (h_mean, a_mean, a_median, o_code, l_code) VALUES ({}, {}, {}, '{}', {})".format(
        h_mean, a_mean, a_median, o_code, l_code
        )
    cursor.execute(sql)

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
    
    database = "occupation"
    condition = "o_code = {}".format(job_id) # what's the column name?

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

def update_occupation(cursor, o_code, o_name):
    sql = "UPDATE occupation SET o_name = '{}' WHERE o_code = '{}'".format(
        o_name, o_code
        )
    cursor.execute(sql)
def update_salary(cursor, h_mean, a_mean, a_median, o_code, l_code):
    sql = "UPDATE salary SET h_mean = {}, a_mean = {}, a_median = {} WHERE o_code = '{}' AND l_code = {}".format(
        h_mean, a_mean, a_median, o_code, l_code
        )
    cursor.execute(sql)

#add this little function to the definition
def search(cursor):

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
        return search(cursor)
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
    m_code = get_occupation_code(cursor, majorName) # only care about first two digits in the code
    m_code2 = (str)((int)(m_code) + 1)
    l_name = convert_location(cursor, locName) # get a valid location
    if m_code == None:
        return None
    if l_name == None:
        return None
    sql = "SELECT DISTINCT AVG(h_mean) FROM salary, major, location, occupation WHERE salary.o_code = occupation.o_code AND (salary.o_code >= '{}' AND salary.o_code <= '{}')".format(m_code, m_code2)
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

def top5(cursor):
    print ("What's your location?")
    loc = input()
    query = ("SELECT DISTINCT o_name, h_mean, a_mean, a_median FROM salary, major, location, ocupation " 
            "WHERE l_name LIKE '%s%' AND occupation.o_code = salary.o_code AND occupation.o_code => m_code AND occupation.o_code < (m_id + 1) ORDER BY h_mean DESC LIMIT 5"
        )
    cursor.execute(query, (m,a))
    return cursor.fetchall()




def menu():
    print("Enter a command to begin.")
    print("~~~~~~~~~~~~~~~~~~~~")
    print("    Commands:")
##    print("        i : insert new item into database")
    print("        is: insert new salary into database")
    print("        io: insert new occupation into database")
    print("        d : delete occupation by ID")
    print("        ds: delete salary by ID and loc") 
##    print("        u : update")
    print("        us: update salary row")
    print("        uo: update occuptation row")
    print("        s : search occupation name")
    print("        ss: search for a job by major or location")
    print("        o : get occupation code matching job title")
    print("        l : get location code matching location name")
##    print("        I : insert new item into database (each column)")
##    print("        D : delete item satisfying condition")
##    print("        S : select item satisfying condition")
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
            print("Occupation with o_name {} and o_code {} has been inserted.\n".format(inp1, inp))
        
        elif opt=='is':
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
            print("Salary has been inserted into database for occupation with code {}.\n".format(inp3))

        elif opt=='uo':
            print("Enter the o_code")
            inp=input()
            print("Enter the new o_name")
            inp1=input()
            update_occupation(cursor, inp, inp1)
            cnx.commit()
            print("Occupation with o_code {} has been renamed to {}.\n".format(inp, inp1))
        
        elif opt=='us':
            #h_mean, a_mean, a_median, o_code, l_code
            print("Enter the occupation code")
            inp3=input()
            print("Enter the location code")
            inp4=input()
            print("Enter the new hourly mean")
            inp=input()
            print("Enter the new annual mean")
            inp1=input()
            print("Enter the new annual median")
            inp2=input()
            update_salary(cursor, inp, inp1, inp2, inp3, inp4)
            cnx.commit()
            print("Salary data at location with l_code {} with o_code {} has been updated.\n".format(inp4, inp3))

        elif opt=='d':
            print("Enter the o_code of the item to delete:")
            inp=input()
            delete(cursor, inp)
            cnx.commit()
            print("Occupation with o_code {} has been deleted.\n".format(inp))
	
        elif opt=='ds':
            print("Enter the o_code of the salary to delete:")
            inp=input()
            print("Enter the l_code of the salary to delete:")
            inp1=input()
            delete_salary(cursor, "o_code = '{}' AND l_code = '{}'".format(
                inp, inp1) )
            cnx.commit() 
            print("Salary data for o_code {} at location with code {} has been deleted.\n".format(inp, inp1))

        # select by name
        elif opt=='s':
            print("Enter the name of the occupation to search for:")
            inp=input()
            records = select_by_name(cursor, inp)
##            x = prettytable.PrettyTable(["Major name", "Location name", "Hourly mean", "Annual wage", "Annual median"])
##            for row in records:
##                x.add_row( row )
##            print(x)
            print(records)
            print("")

        # search by major / area
        elif opt=='ss':
            records = search(cursor)
            x = prettytable.PrettyTable(["Major name", "Location name", "Hourly mean", "Annual wage", "Annual median"])
            for row in records:
                x.add_row( [ row[0], row[1], row[2], row[3], row[4] ] )
            print(x)
            print("")

        # search avg
        elif opt=='a':
            print("Enter the major name:")
            inp=input()
            print("Enter the location name:")
            inp1=input()
            records = get_hourly_avg(cursor, inp, inp1)
            if records:
                x = prettytable.PrettyTable(["Average hourly wage:"])
                for row in records:
                    x.add_row( [ row[0] ] )
                print(x)
                print("")
            else:
                print("No results.\n")

        # get occupation code from occupation name
        elif opt=='o':
           print("Enter the name of occupation:")
           inp=input()
           results = select_o_code(cursor, inp)
           print(results)
           print("")
    
        elif opt=='l':
           print("Enter a valid location e.g. 'Tallahassee, FL':")
           loc=input()
           area_code = get_location_code(cursor, loc)
           print(area_code)
           print("")
           
        elif opt == 't':
            records = top5(cursor)
            x = prettytable.PrettyTable(["Occupation name", "Mean wage", "Annual wage", "Median wage"])
            for row in records:
                x.add_row( [ row[0], row[1], row[2], row[3]] )
            print(x)
                
        elif opt=='q':
            programIsRunning=False
    # end while
    
    cnx.commit()
    cursor.close()
    cnx.close()
# end main
            

if __name__=="__main__":
    main()


    
