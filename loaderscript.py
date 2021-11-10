"""
	Script to interact with a POSTGRES database.
  Remember to check the SETTINGS section to insert your database access data.
	
  
  TODO
	[ ] Some prints and outputs have a comment like `# VERBOSE?` and could be 
		printed to screen only if a verbose flag is passed (like the sql one)
	[ ] When `--dir`, should I tell the user if some of the tables specified in "sequence"
		are missing from the folder?
"""


# --- SETTINGS ---
"""
You can store the access data to the database here, 
so you don't need to type it everytime you execute the script.

Those variables are used as defaults by the argparse parser.
"""
dbname = ""
user = ""
pwd = ""
host = "localhost"
saveSQL = False

# Sequence used to determine the tables order when loading from a folder.
# The names are those of the table in the database, taken from the file header.
sequence = ["reference",
	"technique",
	"regionType",
	"disease",
	"rnaBiology",
	"biologyFunction",
	"organism",
	"gene",
	"transcript",
	"rg4bp",
	"bindingSite",
	"rg4",
	"rg4bpToRg4",
	"rg4bpToPhenotype",
	"diseaseToGene",
	"rg4ToTranscript",
	"rg4ToGene",
	"bindingSiteToTranscript",
	"bindingSiteToGene"]


# --- IMPORTS ---

from os import path, listdir, popen
from sys import exit
import argparse
import psycopg2


# --- FUNCTIONS ---

# SQL Queries executor
def executeQuery(cursor, sql, logSkip = False):
	"""
	If logSkip = True is passed when using the function
	that command will NOT be in the SQL log
	"""

	try:
		cursor.execute(sql)
		if args.saveSQL and not logSkip:
			outfile.write(sql + "\n")
		return cursor
	except Exception as e:
		if args.saveSQL:
			outfile.close()
		print("> ERROR: " + str(e))
		print("> SQL: " + sql)
		closeConnection()
		exit(1)

# Single table loader
def loader(inputFile):
	"""
	Uploads the table in the inputFile with an UPSERT function to the POSTRGES database
	"""

	global outfile
	if args.saveSQL:
		outfile = open("{}.sql".format(inputFile), "w+")	

	keyColumns = []
	columnNames = []
	runOnce = 0         # for the file tests

	# skip a line after insertion of the file/directory
	print("")

	with open(inputFile, "r") as infile:
		for line in infile:
			# get keyColumns and the columnNames of the file
			if line.startswith('#'):
				line = line[1:].strip().split("\t")
				keyColumns = line[1:]
				databaseTable = line[0]
				print("Updating table {}.".format(databaseTable))
			
			elif line.startswith('@'):
				columnNames = line[1:].strip().split("\t")

			else:
				if runOnce == 0:
					# checks presence of key and column headers
					if len(keyColumns) == 0 or len(columnNames) == 0:
						print("""\tERROR: No keys or columns were provided, please check that the file has:\n\t#tableName\tkey1\tkey2 etc.\n\t@column1\tcolumn2\column3 etc.\n""")
						exit(1)
					
					# checks that database table columns and provided ones are the same number
					executeQuery(cursor, "select * FROM {} LIMIT 0".format(databaseTable), logSkip = True)
					tableColumns = [desc[0] for desc in cursor.description]
					if len(columnNames) != len(tableColumns):
						print("\tERROR: The number of columns in the provided file table is different from the number of columns in the database table, please check the file.")
						print("\tThe columns of the selected table ({}) in the database are: \n\t\t{}".format(databaseTable, tableColumns))
						print("\tWhile the provided ones were: \n\t\t{}".format(columnNames))
						exit(1)
					
					# Check if provided keys are in the selected database table
					for key in keyColumns:
						if key not in tableColumns:
							print("\tERROR: The keys provided in the '#' line are NOT present in the database table. Please check them.")
							exit(1)
					
					pos = [columnNames.index(i) for i in keyColumns]
					runOnce = 1
				
				# -- UPSERT --
				# ============
        # To understand why the replace, see the block of comments below
				valuesFromLine = line.strip().replace("'", "prime").split("\t")
				
				whereSql = ["{}='{}' AND ".format(col, entr) for col, entr in zip(keyColumns, [valuesFromLine[i] for i in pos])]
				whereSql = "".join(whereSql)[:-5]
					
				# some strings have ' in them. That breaks the '{}' in here, trying to fix by swapping apexes and doublequotes.
				#     that does NOT work, need singlequotes for the sql to work apparently..
				# to escape ' in SQL you apparently have to double them: ''
				#     did NOT work. 
        # Changed all ' into "prime" and good like that.
				setSql = ["{}='{}',".format(col,entr) for col,entr in zip(tableColumns, valuesFromLine)]
				setSql = "".join(setSql)[:-1] # remove last comma

				insSql = """INSERT INTO {} VALUES ({}) ON CONFLICT ({}) WHERE {} DO UPDATE SET {}""".format(databaseTable, str(valuesFromLine)[1:-1], ", ".join(keyColumns), whereSql, setSql)
				executeQuery(cursor, insSql)

	# Commit the changes!
	conn.commit()
	print("\t{} table updated.".format(databaseTable))

	# Close the logfile
	if args.saveSQL:
		outfile.close()

# Multiple file sorter and loader
def sequenceLoad(fileNames, sequence):
	"""
	Gets a list of fileNames as input, and a sequence with the order to follow.
	Checks all the provided files, if it seems like a table file (headers)
	keeps it in a queue to be loaded.
	Then pass the files one by one, in the correct order, to the loader function.

	NOTE: In the directory there MUST be only one file for each table!
			(haven't tested what happens, probably the table gets re-loaded)
	"""

	# check if the file resembles a table file, if it does
	# lookup the name of the table in the seq list, and memorize it's "place"
	# then load the files one by one with the loader function.
	queue = []

	for fileName in fileNames:
		with open(args.dir + fileName, "r") as infile:
			first = infile.readline()
			second = infile.readline()

			if first[0] == "#" and second[0] == "@":
				tableName = first.split("\t")[0][1:]
				# seq.index(tableName) is the position of the table in the "order" (seq list)
				try:
					ind = sequence.index(tableName)
					queue.append((ind, args.dir + fileName))
				except:
					print("""{} seems like a file for the {} table.\n\tBut that table isn't in the sequence list.\n\tExecution will continue.\nIf this is an error check the sequence list in the script settings""".format(fileName, tableName))
      
			else:
				continue

	queue.sort()

	for element in queue:
		loader(element[1])

# Close connection to database
def closeConnection():
	if conn is not None:
		cursor.close()
		conn.close()
		print("\nDatabase connection closed.")

# Delete all tables from the database
def deleteTables(cursor):
	"""
  Deletes the existing tables and all their contents
  """
	
  print("\nRemoving tables...")

	for table in sequence:
		sql = "DROP TABLE {} CASCADE;".format(table)		
		
		try:
			cursor.execute(sql)			
			print("Table {} removed".format(table))
		except Exception as e:
			print("> SQL: " + sql)
			print("> WARNING: " + str(e)[:-1]) # removing the newline char for formatting			
			# Rollback to bypass the block of the cursor
			print("Rolling back...\n")	# VERBOSE?
			cursor.execute("rollback;")

	conn.commit()
	return 0	

# Reload all tables from model.txt
def reloadTables(modelsfile):
	"""
  Reloads the empty tables from a models.txt file
  """
	
  print("\nReloading tables...")	
	comm = popen("PGPASSWORD={} psql --host={} -U {} -d {}  < {}".format(args.pwd, args.host, args.user, args.dbname, modelsfile))
	print(comm.read())	# VERBOSE?

	return 0


# --- MAIN ---

if __name__ == "__main__":

	# Parser, in here so that you can import the functions above
	parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, 
	description="""
 Welcome to the POSTGRESQL AUTOLOADER.
 Remember to insert/check your database access info in the script -- SETTINGS -- section""", add_help=True)
	parser._action_groups.pop()
	access = parser.add_argument_group('Database access arguments')
	mode = parser.add_argument_group('Exclusive Required arguments (choose ONLY one)')
	optional = parser.add_argument_group('Optional arguments')

	# access to the database
	access.add_argument('--host', default=host, type=str,
		help='The host where the postgres database is')
	access.add_argument('-d', '--dbname', type=str, 
		help='The database\'s name', required = False, default = dbname)
	access.add_argument('-U', '--user', type=str, 
		help='The user as which to access the database', required = False, default = user)
	access.add_argument('-p', '--pwd', type=str, 
		help='The database password for the specified user', required = False, default = pwd)

	# optionals and loading mode
	mode.add_argument('-f', '--tablefile', type=str,
		help="Load a single fileTable to the database.", required=False)
	mode.add_argument('-D', '--dir', type=str,
		help="Detect and load all the tables in the directory", required=False)
	mode.add_argument('--reloadTables', metavar="modelsfile", type=str,
		help="Delete all tables and reload them from the models file", required=False)
	optional.add_argument('--saveSQL', action='store_true', 
		help="Save a logFile of the SQL commands executed by the script, ONE FOR EACH fileTable", required=False)

	args = parser.parse_args()

	# check for necessary arguments
	# there is parser.add_mutually_exclusive_group(required=True), but it does't show args in the help
	"""
	if not (args.tablefile or args.dir or args.reloadTables):
		parser.print_help()
		print("\nERROR: one of the arguments --file --dir is Required.\n")
		exit(1)
	if args.tablefile and args.dir:
		print("\nERROR: ONLY ONE of the arguments --file --dir can be used at the same time.\n") 
		exit(1)
	"""
	# I like the idea of connecting only when necessary. So in the loader function.
	# But if loading multiple files we don't want to keep opening and closing the connection.
	conn = psycopg2.connect(host=args.host, database=args.dbname, user=args.user, password=args.pwd)
	cursor = conn.cursor()

	print("\nConnected to database {} as user {}.".format(args.dbname, args.user))

	# if --file flag
	if args.tablefile:
		if not path.isfile(args.tablefile):
			print("\nThe provided file was not found, please check that the name was correct.")
			closeConnection()
			exit(1)
		else:
			loader(args.tablefile)

	# if --dir flag you will also have the directory path
	if args.dir:
		if not args.dir.endswith("/"):
			args.dir += "/"
		if not path.isdir(args.dir):
			print("\nThe provided directory was not found, please check that the name was correct.")
			closeConnection()
			exit(1)
		else:
			sequenceLoad(listdir(args.dir), sequence)

	# If --reloadtables, we have the SQL models in modelsfile
	if args.reloadTables:
		# Warn user that the tables will be deleted from the database
		if input("All the tables in the database will be removed, are you sure you want to continue? [y/N]: ") in ["y", "Y", "yes", "Yes"]:
			# Execute main function
			deleteTables(cursor)
		else:
			print("Understood. Aborting.")
			exit(1)
		
		# Load the new tables in the databse
		reloadTables(args.reloadTables)


	# close connection to the database
	closeConnection()
