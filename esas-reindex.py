import base64
import csv
import datetime
import http.client
import json
import os
import re
import ssl
import sys
import urllib

# Internal --

required = True
optional = False
lookupCache = {}

# Configuration --

# The username and password is taken from a password manager. You can replace
# the values with plain text values such as 'esas-tst\cphtestuser' and
# 'somepassword' or pass them values in from the command line.

configuration = {
    'outfile': '/tmp/reindex.csv',
    'host': "integration-esas.test.ufmit.dk",
    'base': "/odata/",
    'certificate_file': '/Volumes/home/Documents/Credentials/Funktionssignatur/esas int 2020/myfile.pem',
    'certificate_secret': '',
    'user': os.popen('security find-generic-password -s esasTEST | grep acct | sed  "s/.*\\(esas.*test\\).*/\\1/g" | sed "s/134//g"').read().replace("\n", ""),
    'pass': os.popen('security find-generic-password -w -s esasTEST').read().replace("\n", ""),
}

reindexFields = [
    ('cprnr', 'esas_person.esas_cpr_nummer', required),
    ('fornavn', 'esas_person.FirstName', required),
    ('efternavn', 'esas_person.LastName', required),
    ('Addresse1', 'esas_person.Address1_Line1', required),
    ('Addresse2', 'esas_person.Address1_Line2', optional),
    ('Zip', 'esas_person.postnummer', optional),
    ('City', 'esas_person.by', optional),
    ('email', 'esas_studieemail', required),
    ('mobil', 'esas_mobiltelefonnummer', optional),
    ('stam klasse', 'hold', required),
    ('hold', '', optional),
    ('alternativt id', '', optional),
    ('Afdeling', 'afdeling', required),
]

esasQueries = {
    'Personoplysning': 'Personoplysning?$filter=statecode%20eq%200%20and%20esas_rolle%20eq%20742980000&$select=esas_integration_id,esas_studieemail,esas_kaldenavn,esas_mobiltelefonnummer&$expand=esas_person($select=esas_navne_addressebeskyttet,Address1_Line1,Address1_Line2,esas_cpr_nummer,FirstName,LastName,ContactId,esas_postnummer_by_id)',
    'Studieforloeb': 'Studieforloeb?$filter=statuscode%20eq%20742980001%20and%20statecode%20eq%200%20and%20esas_stamhold_id%20ne%20null&$select=esas_afdeling_id,esas_studerende_id&$expand=esas_stamhold($select=esas_navn)',
    'Afdeling': 'Afdeling?$filter=statecode%20eq%200&$select=esas_navn',
    'Postnummer': 'Postnummer?$filter=statecode%20eq%200&$select=esas_by,esas_postnummer',
}

# Functions --


def nameFromAlias(alias):
    names = alias.split(' ')
    return (names.pop(0), ' '.join(names))


def pathValue(json, path):
    o = json
    for part in path.split('.'):
        if part in o:
            o = o[part]
        else:
            return ''
    return o


def isSingleResult(responseJson):
    return re.search(r'entity$', responseJson['@odata.context'])


def parseNextLink(config, nextLink):
    parsed = urllib.parse.urlparse(nextLink)
    return re.sub(r'^'+config['base'], '', parsed.path)+'?'+parsed.query.replace(' ', '%20')


def lookupValue(data, entity, returnField, lookupKey, lookupValue):
    global lookupCache
    cacheKey = ":".join([entity, returnField, lookupKey])
    if not cacheKey in lookupCache:
        lookupCache[cacheKey] = {}
        for obj in data[entity]:
            lookupCache[cacheKey][pathValue(
                obj, lookupKey)] = pathValue(obj, returnField)
    if not lookupValue or not lookupValue in lookupCache[cacheKey]:
        return ''
    return lookupCache[cacheKey][lookupValue]


def writeCSVFile(config, reindexFields, data):
    csvData = [list(map(lambda x: x[0], reindexFields))]

    for person in data['Personoplysning']:
        row = []
        for (fieldName, path, isRequired) in reindexFields:
            value = pathValue(person, path)
            if isRequired and not value:
                continue
            row.append(value)
        csvData.append(row)

    with open(config['outfile'], 'w', encoding='utf-8') as file:
        writer = csv.writer(file, delimiter=';', quoting=csv.QUOTE_ALL)
        writer.writerows(csvData)


def performQueryInternal(config, query):
    url = config['base']+query
    user = config['user']+':'+config['pass']
    userEncoded = base64.b64encode(user.encode()).decode()

    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Basic {}'.format(userEncoded)
    }

    context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
    context.load_cert_chain(
        certfile=config['certificate_file'],
        password=config['certificate_secret'])

    connection = http.client.HTTPSConnection(
        config['host'], port=443, context=context)

    connection.request(method="GET", url=url, headers=headers)

    response = connection.getresponse()
    if response.status != 200:
        raise Exception("Query returned status "+str(response.status) +
                        ':'+response.reason+' for query: ' + query)
    return response.read()


def performQuery(config, query):
    allJson = json.loads('[]')
    nextQuery = query
    while nextQuery:
        j = json.loads(performQueryInternal(config, nextQuery))
        nextQuery = parseNextLink(config, j['@odata.nextLink']) if '@odata.nextLink' in j else ''

        if isSingleResult(j):
            allJson.append(j)
        else:
            for i in j['value']:
                allJson.append(i)

    return allJson

# Startup --

print("Processing began at", datetime.datetime.now().strftime("%c"))

if sys.version_info[0] < 3:
    print("This script was developed using Python 3. Please run it with the same Python version.")
    exit(1)

# Retrieve data --

data = {}
for key, q in esasQueries.items():
    print("Retrieving", key, '...')
    data[key] = performQuery(configuration, q)
    print("Retrieved", len(data[key]), 'entries')

# Process data --

for i in range(len(data['Personoplysning'])-1, 0, -1):
    p = data['Personoplysning'][i]

    # Locate id's

    personId = pathValue(p, 'esas_person.ContactId')
    deptId = lookupValue(data, 'Studieforloeb', 'esas_afdeling_id', 'esas_studerende_id', personId)
    zipCodeId = pathValue(p, 'esas_person.esas_postnummer_by_id')

    # Check for active class-of

    stamhold = lookupValue(data, 'Studieforloeb', 'esas_stamhold.esas_navn', 'esas_studerende_id', personId)
    if not stamhold:
        data['Personoplysning'].pop(i)
        continue

    # Navnebeskyttelse

    if pathValue(p, 'esas_person.esas_navne_addressebeskyttet'):
        name = nameFromAlias(pathValue(p, 'esas_kaldenavn'))
        p['esas_person']['FirstName'] = name[0]
        p['esas_person']['LastName'] = name[1]

    # Other looked up values

    p['esas_person']['postnummer'] = lookupValue(data, 'Postnummer', 'esas_postnummer', 'esas_postnummerId', zipCodeId)
    p['esas_person']['by'] = lookupValue(data, 'Postnummer', 'esas_by', 'esas_postnummerId', zipCodeId)
    p['hold'] = stamhold
    p['afdeling'] = lookupValue(data, 'Afdeling', 'esas_navn', 'esas_afdelingId', deptId)

    data['Personoplysning'][i] = p

# Write data to csv file --

writeCSVFile(configuration, reindexFields, data)

print("Done.")
