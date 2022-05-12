#! /usr/bin/python3

from mrjob.job import MRJob

class MissingValueCounterMRJob(MRJob):

    DELIMITER = ","
    FILE_HEADER = 'timestamp,activityID,heartrate,handTemperature,handAcc16_1,handAcc16_2,handAcc16_3,handAcc6_1,handAcc6_2,handAcc6_3,handGyro1,handGyro2,handGyro3,handMagne1,handMagne2,handMagne3,handOrientation1,handOrientation2,handOrientation3,handOrientation4,chestTemperature,chestAcc16_1,chestAcc16_2,chestAcc16_3,chestAcc6_1,chestAcc6_2,chestAcc6_3,chestGyro1,chestGyro2,chestGyro3,chestMagne1,chestMagne2,chestMagne3,chestOrientation1,chestOrientation2,chestOrientation3,chestOrientation4,ankleTemperature,ankleAcc16_1,ankleAcc16_2,ankleAcc16_3,ankleAcc6_1,ankleAcc6_2,ankleAcc6_3,ankleGyro1,ankleGyro2,ankleGyro3,ankleMagne1,ankleMagne2,ankleMagne3,ankleOrientation1,ankleOrientation2,ankleOrientation3,ankleOrientation4,subject_id'
    
    def mapper(self, _, row):
        columns = self.FILE_HEADER.split(self.DELIMITER)
        if row != self.FILE_HEADER:
            values = row.split(self.DELIMITER)
            col_count = len(values)
            
            for i in range(col_count):
                yield (columns[i], values[i])
    
    def reducer(self, key, value):
        values = list(value)
        yield (key, values.count(''))
        
if __name__ == '__main__':
     MissingValueCounterMRJob.run()        