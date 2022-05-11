"""

    This script detects the encoding of the file passed
    and alters it to the chosen codec if needed.
    For example:
        UTF-8-SIG -> utf-8
        ascii     -> ascii
    It keeps a copy of the old codec files in ./oldEncodeTables/ just in case.

"""

#
# Maybe a faster way to do this would be to use the file command from bash
# and check the already detected encoding. 
# Maybe iconv -f from_encoding -t to_encoding could be use for the conversion, BUT
# this is Linux specific, while this script should work on all systems.
#

# [ IMPORTS ]
from os import path, mkdir, replace, rename
from sys import exit
from chardet import detect
import argparse

# [ FUNCTIONS ]
def getEncoding(file):
	with open(file, 'rb') as f:
		rawdata = f.read()
	return detect(rawdata)['encoding']


def convertEncoding(file, fromCodec):
	toCodec = "utf-8"
	try:
		with open(infile, 'r', encoding=fromCodec) as i, open("tmpfile", 'w', encoding=toCodec) as o:
			while True:
				contents = i.read() # could add a chunkSize here for very big files
				if not contents:
					break
				o.write(contents)
			
			# save old encoded file in a new directory
			if not path.isdir("./oldEncodeTables/"):
				mkdir("oldEncodeTables")
			else:
				pass
			
			# replace works as rename but replaces the destination file if already present
			replace(infile, "oldEncodeTables/" + infile[:-4] + "_oldEnc.txt") # move old encoding file
			rename("tmpfile", infile) # rename new encoding as old one
			print("The conversion was successful, the file {} should now be in a correct encoding.")
			exit(0)
	
	except UnicodeDecodeError:
		print('Decode Error')
		exit(1)
	except UnicodeEncodeError:
		print('Encode Error')
		exit(1)


# [ MAIN ]
if __name__ == "__main__":
	
	# Parser
	parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, 
	description="""
 Welcome to the convertEncoding script.
 You only need to pass to this script the "table file" you think is in the wrong encoding
 and it try to convert it in a working one.""", add_help=True)

	# file to be converted
	parser.add_argument('-f, --file', action='store', type=str, required=True,
		help="The table file to be checked.")
	
	args = parser.parse_args()
	
	# check that the passed file is correct
	if not os.path.isfile(path):
		print("The provided file was not found, please check that the name was correct.")
		exit(1)
	else:
		# get the current Encoding of the file
		fromCodec = getEncoding(args.file)
		print("Detected encoding: {}.".format(fromCodec))
		
		# those are the two encodings we know work, if not one of those
		# we convert
		if fromCodec not in ["utf-8", "ascii"]:
			print("Converting the file...")
			convertEncoding(args.file, fromCodec)
			print("Done!")
			exit(0)
		else:
			print("This file should already be correct.")
			exit(1)
