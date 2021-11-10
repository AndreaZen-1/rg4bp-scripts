# rg4bp-scripts
This repo is a collection of small scripts that may prove useful in the upkeep and testing of the `RG4DB` database.

The main script is `loaderscript.py`, its main functions are:
 - Connecting to a POSTGRES database
 - Removing tables of the database as specified in a list in the script
 - Loading tables in the database from a models.txt file (with the SQL definition of the tables)
 - Populating the tables of the database by a single file or a directory (again, using a list in the script)

## Usage example:
 0. Insert the data to access your POSTGRES database in the [ SETTINGS ]
 1. Remove and reload the tables on the database with `--reloadTables models.txt`
 2. Inspect the database with django (`python manage.py inspectdb`) -> `models.py`
 3. Order the models.py with the `order_models.py` script
 4. Run the table_loader script to populate the tables (`--dir tables_folder/`)

### Future improves
[ ] Some prints and outputs have a comment like `# VERBOSE?` and could be printed to screen only if a verbose flag is passed (like the sql one)
[ ] When `--dir`, should I tell the user if some of the tables specified in "sequence" are missing from the folder?
