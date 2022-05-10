# rg4bp-scripts
This repo is a collection of small scripts that may prove useful in the upkeep and testing of the `RG4DB` database.

The main script is `loaderscript.py`, its main functions are:
- Connecting to a POSTGRES database
- Removing tables of the database as specified in a list in the script
- Loading tables in the database from a models.txt file (with the SQL definition of the tables)
- Populating the tables of the database by a single file or a directory (again, using a list in the script)

## Structure of a table file
A "table file" is a TAB-separated file (usually .txt) containing the information to be loaded in a certain table in the database.
In order for the script to correctly load the data respecting the keys and relationships, those files need to follow a certain structure:
```
#tableName /TAB/ keyName
@column1 /TAB/ column2 /TAB/ column3 etc...
data /TAB/ data /TAB/ data etc...
...
```
The first two lines are referred as "headers", key header and column header, while all the following lines contain the data that needs to be uploaded in the databse.
Notice that the data is divided by columns and needs to respect the definition of the column in the table (SQL definition in the models.txt file).
Also, the information in the headers (keys and column names) will be compared with the keys and columns in the corresponding table in the database. If the keys are not present or the columns do not match you will be notified.


## Usage example:
1. Insert the data to access your POSTGRES database in the [ SETTINGS ]
1. Remove and reload the tables on the database with `--reloadTables models.txt`
1. Inspect the database with django (`python manage.py inspectdb`) -> `models.py`
1. Order the models.py with the `order_models.py` script
1. Run the table_loader script to populate the tables (`--dir tables_folder/`)

### To-Dos
- [ ] Make a separate function to check the correctness of a table file (instead of checking each line in the loading function)
- [ ] check the encoding of the file, and if not the one we want alter it.
- [ ] Some prints and outputs have a comment like `# VERBOSE?` and could be printed to screen only if a verbose flag is passed (like the sql one)
- [ ] When `--dir` is used, should I tell the user if some of the tables specified in "sequence" are missing from the folder?
- [ ] Should be able to make the upload of large files faster by using the `psycopg2.extras.execute_values()` execution helper. Need to benchmark.
