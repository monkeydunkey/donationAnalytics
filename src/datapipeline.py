import multiprocessing
import math
import time
import json
from integrityChecks import IntegrityChecks

configFilePath = 'src/config.json'

def read_config():
    """
    Reads the config file
    """
    try:
        data = None
        with open(configFilePath) as w:
            data = json.load(w)
        return data
    except Exception as e:
        print 'ERROR: problem with reading the config file', e
        raise

def preProcessLine(line):
    """
    This function ensure that the input provided satisfies all the constraints required for ensuring data quality
    """
    try:
        line = line.split('|')
        if len(line) != 21: raise "Not the correct number of entries"
        colInd = [0, 7, 10, 13, 14, 15]
        outData = []
        for col in colInd:
            outData.append(line[col])
        #Making sure the zip code is smaller than 5 digits, using string to ensure the logic will
        #work even if zip code does not contains numerical digits
        outData[2] = str(outData[2])[:5]
        checks = IntegrityChecks()
        return outData if checks.run(outData) else None
    except Exception as e:
        print "PARSING ERROR: error in parsing line", line, e
        return None

def processTransactionAmt(val):
    """
    This function processes and ensures that the transaction amount that is printed
    is int if the total is an integer
    """
    #Rounding till second decimal place to ensure that the output will be only till 2 decimal places
    val = round(val, 2)
    return int(val) if val.is_integer() else val

def writeToFile(outLine, configData):
    """
    Writes the data to the output file
    """
    outFileName = configData.get('outFileName', None)
    if outFileName is None:
        raise 'ERROR no output file provided'
    with open(outFileName, 'a') as outfile:
        outfile.write(outLine + '\n')

def fileWorker(dataQueue, configData):
    '''
    This function will read the lines one by one from the input file and put it on the queue
    it will take few microseconds between records to simulate a stream of data
    '''
    inputFileName = configData.get('inputFileName', None)
    if inputFileName is None:
        raise 'ERROR no input file provided'
    with open(inputFileName, 'r') as infile:
        for line in infile:
            dataQueue.put(line)
            #Sleeping for 1 millisecond to simulate streaming
            time.sleep(0.0001)

def closeServive(campaignInfo, contributorInfo, configData):
    """
    It stores the current contributorInfo and campaignInfo data to a file if one
    is specified
    """
    if configData.get('contributorInfo', None) is not None:
        with open (configData['contributorInfo'], 'w') as d:
            json.dump(contributorInfo, d)

    if configData.get('campaignInfo', None) is not None:
        with open (configData['campaignInfo'], 'w') as d:
            json.dump(campaignInfo, d)

def instantiateService(configData):
    """
    Read the previous saved session information if any else it creates and returns
    new variables for storing the data requried for calculating repeat donors
    and their contribution

    """
    #TODO: make sure if there is a entry present then we have to create the file
    contributorInfo, campaignInfo = {}, {}
    if configData.get('contributorInfo', None) is not None:
        try:
            with open (configData['contributorInfo'], 'r') as d:
                contributorInfo = json.load(d)
        except IOError as e:
            contributorInfo = {}

    if configData.get('campaignInfo', None) is not None:
        try:
            with open (configData['campaignInfo'], 'r') as d:
                campaignInfo = json.load(d)
        except IOError as e:
            campaignInfo = {}
    return campaignInfo, contributorInfo

def getPercentileContribution(contributions, percentile):
    """
    Nearest Rank Percentile contribution as specified here - https://en.wikipedia.org/wiki/Percentile
    """
    contributions = sorted(contributions)
    index = int(math.ceil(percentile * len(contributions)/ 100)) - 1
    return int(round(contributions[index]))

def checkRepeatContributor(campaignInfo, contributorInfo, lineData, percentileCutOff):
    """
    Checks if the contributor was a repeat contributor and outputs the string
    that needs to be printed to the output file
    """
    campaignID, name, zipCode, tranDt, tranAmt, otherID = lineData
    tranYear = tranDt[-4:]
    contributorKey = '|'.join(map(lambda x: x.strip(), [name, zipCode]))
    campaignKey = '|'.join(map(lambda x: x.strip(), [campaignID, zipCode, tranYear]))
    retStr = None
    if contributorInfo.get(contributorKey, None) is None:
        contributorInfo[contributorKey] = [int(tranYear)]
    elif len(filter(lambda x: x < int(tranYear), contributorInfo[contributorKey])) == 0:
        #this entry is out of order
        contributorInfo[contributorKey].append(int(tranYear))
    else:
        #this is a repeat contributor so we have to print the info now
        campaignInfo[campaignKey] = campaignInfo.get(campaignKey, []) + [float(tranAmt)]
        contributorInfo[contributorKey].append(int(tranYear))
        retStr =  '|'.join(
                    map(lambda x: str(x).strip(), [campaignID, zipCode, tranYear,
                        getPercentileContribution(campaignInfo[campaignKey], percentileCutOff),
                        processTransactionAmt(sum(campaignInfo[campaignKey])),
                        len(campaignInfo[campaignKey])
                    ]))
    return retStr

def service(dataQueue, configData):
    '''
    The service function to run the service part of the process. This will read
    data from the dataQueue and process it one by one in a streaming fashion
    Input:
        dataQueue: Queue on which the line input can be read
        configData: Config file containing the info on how to process data
    '''
    emptyCount = 0
    try:
        #If no schedule was provided then run the code indefinately
        schedule = configData['schedule'] if configData.get('schedule', None) is not None else float("inf")
        sleepTime = configData['serviceSleepTime'] if configData.get('serviceSleepTime', None) is not None else 2

        if configData.get('schedule', None) is None: print ('INFO No schedule was provided, running the service indefinately')
        if configData.get('serviceSleepTime', None) is None: print ('INFO No serviceSleepTime was provided, setting time to 2 sec')

        campaignInfo, contributorInfo = instantiateService(configData)

        while emptyCount < schedule:
            if dataQueue.empty():
                emptyCount += 1
            else:
                #print 'processing approximately', dataQueue.qsize(), 'lines of data'
                while not dataQueue.empty():
                    #Process the lines one by one
                    try:
                        validContributorData = preProcessLine(dataQueue.get())
                        if validContributorData is not None:
                            toPrint = checkRepeatContributor(campaignInfo, contributorInfo, validContributorData, configData['percentile'])
                            if toPrint is not None:
                                writeToFile(toPrint, configData)
                    except Exception as e:
                        print "There was an exception is processing the line", e
            #Sleep the process till the next
            time.sleep(sleepTime)

        #The Service is exiting save the campaign and contributor data if specified in the config
        closeServive(campaignInfo, contributorInfo, configData)

    except Exception as e:
        raise

def readPercentile(configData):
    inputFileName = configData.get('percentileFile', None)
    if inputFileName is None:
        raise 'ERROR no percentileFile provided'

    with open(inputFileName, 'r') as infile:
        configData['percentile'] = float(infile.read().strip())

if __name__ == '__main__':
    try:
        configData = read_config()
        if configData is None:
            print "config file is empty exiting bye"
        else:
            readPercentile(configData)
            queue = multiprocessing.Queue()
            service = multiprocessing.Process(name='service', target=service, args=(queue, configData))
            fileReader = multiprocessing.Process(name='filereader', target=fileWorker, args=(queue, configData))
            service.start()
            fileReader.start()

            fileReader.join()
            service.join()
    except Exception as e:
        print 'ERROR encountered:', e
        print 'Exiting bye'
