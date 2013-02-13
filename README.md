tibco
=====

Parses and processes UK Power Network statistics


Main work is done by TibParser in parser.py and various IO functions
in iosql.py. handler will be renamed and rewritten into some kind of
launcher. tools.py mainly holds useful information and tools.

Basic plan:
Tell CLI what you want:
"python tibco subject=fpn filter=drax --start 2011-01-01 end 2011-12-31 --graph"

Then it first looks in date/store.h5 for data.

exists --> return dataframe (interactive) or plot (standalone)

not exist --> look in data/tibgz/ for gzipped files. 

	exist --> open and parse with TibParser, create pandas DataFrame, save to 
		h5store (unless otherwise specified) and go up one

	not exist --> download from bmreports website and go up one