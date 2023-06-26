import requests
import csv
import bs4 as bs
import multiprocessing as mp
import itertools

#save list of dictionaries to csv
def saveCSV(listOfData, filename):
    outputFile = open(filename, 'w', newline='')
    outputWriter = csv.writer(outputFile)
    outputWriter.writerow(list(listOfData[0].keys()))
    for dataDict in listOfData:
        outputWriter.writerow(list(dataDict.values()))
    outputFile.close()

#API key
headers = {
    'X-API-Key': 'vD854erY2FxNwhKozppPqD1vsGShyejhv0vJXtiQ',
    'Content-Type': 'application/json',
    'Accept': 'application/json'
}

billSlimKeys = ['congress', 'bill_id', 'bill_slug', 'bill_type',
                'sponsor_title', 'sponsor_id', 'sponsor_name',
                'sponsor_state', 'sponsor_party']

# DESCRIPTION
def getCosponsorship(congress, bill_slug):
    requestUrl = 'https://api.propublica.org/congress/v1/' + \
                 str(congress) + '/bills/' + bill_slug + '/cosponsors.json'
    try:
        cosponsorshipPull = requests.get(requestUrl, headers=headers).json()['results'][0]
        billDictSlim = {k: cosponsorshipPull[k] for k in billSlimKeys}
        cosponsorDicts = cosponsorshipPull['cosponsors']
        for cosponsorDict in cosponsorDicts:
            if 'withdrawn_date' not in cosponsorDict.keys():
                cosponsorDict.update({'withdrawn_date': 'n/a'})
            cosponsorDict.update(billDictSlim)
    except:
        print(bill_slug)
        cosponsorDicts = []
    return cosponsorDicts


# DESCRIPTION
def getMembers(congress, chamber):
    requestUrl = 'https://api.propublica.org/congress/v1/' + \
                 str(congress) + '/' + chamber + '/members.json'
    memberPull = requests.get(requestUrl, headers=headers).json()['results'][0]
    members = memberPull["members"]
    for member in members:
        member.pop("next_election", None)
    return members


# get cosponsor edges associated with a specific amendment
def getAmendmentCosponsorDicts(amendmentDictSlim, url):
    r = requests.get(url)
    contents = r.text
    soup = bs.BeautifulSoup(contents, 'html.parser')

    dictList = []

    if soup.find('p', text='No cosponsors.') is None and soup.find('span', text='Cosponsors:') is not None:
        urlTags = soup.find('span', text='Cosponsors:').find_next('table').findAll('a')
        dateTags = soup.find('span', text='Cosponsors:').find_next('table').findAll('td', class_='date')
        cosponsorCount = len(urlTags)
        for i in range(cosponsorCount):
            cosponsorUrl = urlTags[i].attrs['href']
            cosponsor_id = cosponsorUrl[len(cosponsorUrl) - 7:]
            cosponsorDict = {'cosponsor_url':cosponsorUrl, 'cosponsor_id':cosponsor_id,
                             'date':dateTags[i].text.strip()}
            cosponsorDict.update(amendmentDictSlim)
            dictList.append(cosponsorDict)
    else:
        cosponsorCount = 0

    return(cosponsorCount, dictList)


# Get all amendment data associated with a single bill
def pullAmendmentData(bill_slug, congress):
    amendments = []
    amendmentCosponsors = []

    amendmentsUrlNoOffset = 'https://api.propublica.org/congress/v1/' + \
                         str(congress) + '/bills/' + bill_slug + '/amendments.json'

    curOffset = 0
    amendmentsUrlWithOffset = amendmentsUrlNoOffset + '?offset=' + str(curOffset)
    amendmentsPull = requests.get(amendmentsUrlWithOffset, headers=headers).json()['results'][0]

    dontStopNow = True
    while amendmentsPull['num_results'] > 0 and dontStopNow:
        dontStopNow = amendmentsPull['num_results'] == 20
        amendmentsList = amendmentsPull['amendments']

        for amendmentDict in amendmentsList:
            amendmentDictSlim = {"bill_amended":bill_slug, "congress":congress}
            amendmentDict.update(amendmentDictSlim)

            amendmentDictSlim.update({'amendment_number': amendmentDict['amendment_number'],
                                      'sponsor_id': amendmentDict['sponsor_id'],
                                      'introduced_date': amendmentDict['introduced_date']})

            congressDotGovUrl = amendmentDict['congressdotgov_url'].replace('text','cosponsors')

            cosponsorCount, dictList = getAmendmentCosponsorDicts(amendmentDictSlim, congressDotGovUrl)
            print(bill_slug + ' - ' + amendmentDict['amendment_number'] + ' - ' + str(cosponsorCount))

            if cosponsorCount > 0: amendmentCosponsors.extend(dictList)

            amendmentDict.update({'cosponsors':cosponsorCount})
            amendments.append(amendmentDict)

        curOffset = curOffset + 20
        amendmentsUrlWithOffset = amendmentsUrlNoOffset + '?offset=' + str(curOffset)
        amendmentsPull = requests.get(amendmentsUrlWithOffset, headers=headers).json()['results'][0]

    return(amendments, amendmentCosponsors)


# DESCRIPTION
def getData(congress, getBillCosponsors=True, getAmendmentData=True):
    billType = "active"
    chamber = "both"
    requestUrlNoOffset = 'https://api.propublica.org/congress/v1/' + \
                         str(congress) + "/" + chamber + '/bills/' + \
                         billType + '.json'

    global bills, cosponsors, amendmentsAll, amendmentCosponsorsAll
    bills = []
    amendmentsAll = []
    amendmentCosponsorsAll = []

    print("getting bills data")
    curOffset = 0
    requestUrlWithOffset = requestUrlNoOffset + '?offset=' + str(curOffset)
    billsPull = requests.get(requestUrlWithOffset, headers=headers).json()['results'][0]

    while billsPull['num_results'] > 0:
        print(curOffset)
        billsList = billsPull['bills']
        print(billsList[0]['latest_major_action_date'])
        for billDict in billsList:
            billDictToAppend = {k: v for k, v in billDict.items() if (type(v) is int or type(v) is str or v is None)
                                and k not in ['summary']}
            bills.append(billDictToAppend)

        invalidJSON = True
        while invalidJSON:
            curOffset = curOffset + 20
            try:
                requestUrlWithOffset = requestUrlNoOffset + '?offset=' + str(curOffset)
                billsPull = requests.get(requestUrlWithOffset, headers=headers).json()['results'][0]
                invalidJSON = False
            except:
                with open('problemRequests.txt', 'w') as fd:
                    fd.write('congress: ' + str(congress) + ', offset: ' + str(curOffset))
                    fd.close()

    saveCSV(bills, 'bills' + str(congress) + '.csv')

    if getBillCosponsors:
        print("getting cosponsors data")
        pool = mp.Pool(15)
        cosponsorData = pool.starmap(getCosponsorship,
                                     [(congress,
                                       billDict['bill_slug']) for billDict in bills if billDict['cosponsors'] > 0])
        cosponsors = list(itertools.chain.from_iterable(cosponsorData))
        saveCSV(cosponsors, 'cosponsorship' + str(congress) + '.csv')

    if getAmendmentData:
        print("getting amendment data")
        pool = mp.Pool(15)
        amendmentsData = pool.starmap_async(pullAmendmentData,
                                            [(billDict['bill_slug'], congress) for billDict in bills])
        for amendments, amendmentCosponsors in amendmentsData.get():
            amendmentsAll.extend(amendments)
            amendmentCosponsorsAll.extend(amendmentCosponsors)
        saveCSV(amendmentsAll, 'amendments' + str(congress) + '.csv')
        if len(amendmentCosponsorsAll) > 0:
            saveCSV(amendmentCosponsorsAll, 'amendmentCosponsors' + str(congress) + '.csv')

    saveCSV(getMembers(congress, "senate") + getMembers(congress, "house"), 'members' + str(congress) + '.csv')

#getData(117, getBillCosponsors=True, getAmendmentData=True)

#problem for 116 cosponsors: hr1865

saveCSV(getMembers(117, "senate") + getMembers(117, "house"), 'members' + str(117) + '.csv')