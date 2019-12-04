'''
	CDA 4710
'''
#edit the menu like so
def menu():
    print("~~~~~~~~~~~~~~~~~~~~")
    print("    Commands:")
    print("        i : insert new item into database")
    print("        d : delete item by ID")
    print("        D : delete item satisfying condition")
    print("        u : update")
    print("		   s : search")
    print("        q : quit")

#add this to the elif portion

	elif opt == 's'
		records = top5()
		for row in records:
			print("Major name: ", row[0], )
			print("Occupation name: ", row[1])
			print("Mean wage: ", row[2])
			print("Annual wage: ", row[3])
			print("Median wage: ", row[4], "\n")
	#end search

#add this little function to the definition
def search():

	print(" What do you wanna base your search on?")
    print("~~~~~~~~~~~~~~~~~~~~")
    print("    Commands:")
    print("	m : major")
    print("	a : area")
    print(" i : major and area")

    option=input()
    if option == 'm':
    	print("Enter your major's name")
    	m = input()
    	query = ("SELECT DISTINCT m_name, o_name, h_mean, a_mean, a_median FROM salary, major, location, ocupation"
				"WHERE 	l_name LIKE '%s%' AND occupation.o_code = salary.o_code AND occupation.o_code => m_code AND occupation.o_code < (m_id + 1)")

		cursor.execute(query, (m))

		return cursor.fetchall()

    elif option == 'a':
    	print("Enter your state's name")
    	a = input()
    	query = ("SELECT DISTINCT m_name, o_name, h_mean, a_mean, a_median FROM salary, major, location, ocupation"
				"WHERE m_name LIKE '%s%' AND occupation.o_code = salary.o_code AND occupation.o_code => m_code AND occupation.o_code < (m_id + 1)")

		cursor.execute(query, (a))

		return cursor.fetchall()
    elif option == 'i':
    	print("Enter your major's name")
    	m = input()
    	print("Enter your area's name")
    	a = input()
    	query = ("SELECT DISTINCT m_name, o_name, h_mean, a_mean, a_median FROM salary, major, location, ocupation"
				"WHERE m_name LIKE '%s%' AND l_name LIKE '%s%' AND occupation.o_code = salary.o_code AND occupation.o_code => m_code AND occupation.o_code < (m_id + 1)")

		cursor.execute(query, (m,a))

		return cursor.fetchall()

    else :
    	print("Invalid option")
    	return search()
#end search

######### top 5

elif opt == 't':
	records = top5()
	for row in records:
		print("Major name: ", row[0], )
		print("Occupation name: ", row[1])
		print("Mean wage: ", row[2])
		print("Annual wage: ", row[3])
		print("Median wage: ", row[4], "\n")
#end top5

def top5()

print ("What's your location?")
loc = input()

query = ("SELECT DISTINCT o_name, h_mean, a_mean, a_median FROM salary, major, location, ocupation " 
	"WHERE l_name LIKE '%s%' AND occupation.o_code = salary.o_code AND occupation.o_code => m_code AND occupation.o_code < (m_id + 1) ORDER BY h_mean DESC LIMIT 5"
				)

cursor.execute(query, (m,a))

return cursor.fetchall()