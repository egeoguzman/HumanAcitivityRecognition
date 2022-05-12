#! /usr/bin/python3

from mrjob.job import MRJob
from mrjob.step import MRStep


class VarianceMRJob(MRJob):

    DELIMITER = ","
    FILE_HEADER = 'ankleAcc16_1,ankleAcc16_2,ankleAcc16_3,ankleAcc6_1,ankleAcc6_2,ankleAcc6_3,ankleGyro1,ankleGyro2,ankleGyro3,ankleMagne1,ankleMagne2,ankleMagne3,ankleTemperature,chestAcc16_1,chestAcc16_2,chestAcc16_3,chestAcc6_1,chestAcc6_2,chestAcc6_3,chestGyro1,chestGyro2,chestGyro3,chestMagne1,chestMagne2,chestMagne3,chestTemperature,handAcc16_1,handAcc16_2,handAcc16_3,handAcc6_1,handAcc6_2,handAcc6_3,handGyro1,handGyro2,handGyro3,handMagne1,handMagne2,handMagne3,handTemperature,heartrate,timestamp'

    def _mapper(self, _, row):
        columns = self.FILE_HEADER.split(self.DELIMITER)
        if row != self.FILE_HEADER:
            values = row.split(self.DELIMITER)
            col_count = len(values)
            for i in range(col_count):
                yield (columns[i], float(values[i]))
    def _reducer(self, key, values):
        N = S = S2 = 0
        for v in values:
          N += 1  
          S += v
          S2 += v*v
        S2 = float(S2) / N
        S = float(S) / N
        S = S * S 
        var = (S2-S)
        yield (key, var) 

    def steps(self):
        return [ MRStep(mapper=self._mapper,
                        reducer=self._reducer) ]

if __name__ == '__main__':
    VarianceMRJob.run()