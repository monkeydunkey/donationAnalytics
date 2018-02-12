# Module for ensuring data constraints are satisfied
from datetime import datetime

class IntegrityChecks(object):
    def run (self, line):
        self.testCases = [self.checkOtherId, self.checkTransactionDate, self.checkZipCode,
                          self.checkName, self.checkCampaignID, self.checkTransactionAmt]

        for test in self.testCases:
            if not test(line):
                print test
                return False

        return True


    def checkOtherId(self, line):
        return True if line[-1] == "" else False

    def checkTransactionDate(self, line):
        try:
            parsedDate = datetime.strptime(line[3], '%m%d%Y')
            #Ensuring the date is greater than the start date of the data file
            return True if parsedDate >= datetime.strptime('1970-01-01', '%Y-%m-%d') else False
        except Exception as e:
            #This should be generated if the date was malformed
            print 'PARSING ERROR: Transaction date was malformed', '|'.join(line)
            return False

    def checkZipCode(self, line):
        return True if len(line[2]) == 5 else False

    def checkName(self, line):
        """
        Check to ensure that the name field is not empty and not malformed. Malform check
        ensure that only alphabets are present in the name.
        """
        return False if line[1] == "" or any(char.isdigit() for char in line[1]) else True

    def checkCampaignID(self, line):
        return False if line[0] == "" else True

    def checkTransactionAmt(self, line):
        return False if line[-2] == "" or any(not (char.isdigit() or char == ".") for char in line[-2]) else True
