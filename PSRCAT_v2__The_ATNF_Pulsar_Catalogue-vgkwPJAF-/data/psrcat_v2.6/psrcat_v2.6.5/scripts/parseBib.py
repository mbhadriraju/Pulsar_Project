from pybtex.database.input import bibtex
import sys
import utils

infile = sys.argv[1]
database = sys.argv[2]

parser = bibtex.Parser()
bib_data = parser.parse_file(infile)

f = open("allbibs.txt","a")
f.write(bib_data.to_string('bibtex'))
f.close()

label = list(bib_data.entries.keys())[0]

fields = list(bib_data.entries[label].fields)

con = utils.connect_db(database)
version_id = utils.getVersionID(con)

v1label,title,journal,year,month,volume,number,pages,doi,url = None,None,None,None,None,None,None,None,None,None
for val in fields:
    if val == "title":
        title = bib_data.entries[label].fields[val].strip('{').strip('}')
    elif val == "journal":
        journal = bib_data.entries[label].fields[val]
    elif val == "year":
        year = bib_data.entries[label].fields[val]
    elif val == "month":
        month = bib_data.entries[label].fields[val]
    elif val == "volume":
        volume = bib_data.entries[label].fields[val]
    elif val == "number":
        number = bib_data.entries[label].fields[val]
    elif val == "pages":
        pages = bib_data.entries[label].fields[val]
    elif val == "doi":
        doi = bib_data.entries[label].fields[val]
    elif val == "adsurl":
        url = bib_data.entries[label].fields[val]
    elif val == "v1ref":
        v1label = bib_data.entries[label].fields[val]
allauthors = bib_data.entries[label].persons['author']

if(v1label is None or v1label==""):
    print("ERROR: V1LABELISMISSING")
    exit()

author = ""
for i,auth in enumerate(allauthors):
    if i < len(allauthors)-1:
        author = author+str(auth)+" and "
    else:
        author = author+str(auth)


print(f"v1label = {v1label}")
print(f"label = {label}")
print(f"title = {title}")
print(f"author = {author}")
print(f"journal = {journal}")
print(f"year = {year}")
print(f"month = {month}")
print(f"volume = {volume}")
print(f"number = {number}") 
print(f"pages = {pages}")
print(f"doi = {doi}")
print(f"url = {url}")

citationID = utils.addCitation(con,v1label,label,title,author,journal,year,month,volume,number,pages,doi,url,version_id)
print(citationID)
    
