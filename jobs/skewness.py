#! /usr/bin/python3

from mrjob.job import MRJob

class SkewnessMRJob(MRJob):

    DELIMITER = ","
    FILE_HEADER = 'ankleAcc16_1,ankleAcc16_2,ankleAcc16_3,ankleAcc6_1,ankleAcc6_2,ankleAcc6_3,ankleGyro1,ankleGyro2,ankleGyro3,ankleMagne1,ankleMagne2,ankleMagne3,ankleTemperature,chestAcc16_1,chestAcc16_2,chestAcc16_3,chestAcc6_1,chestAcc6_2,chestAcc6_3,chestGyro1,chestGyro2,chestGyro3,chestMagne1,chestMagne2,chestMagne3,chestTemperature,handAcc16_1,handAcc16_2,handAcc16_3,handAcc6_1,handAcc6_2,handAcc6_3,handGyro1,handGyro2,handGyro3,handMagne1,handMagne2,handMagne3,handTemperature,heartrate,timestamp'
    
#     with open('/home/hduser/kdd99-analysis/outputs/mean.csv') as fin:
#         means = [float(line[line.find("\t")+len("\t"):line.rfind("\n")]) for line in fin]
        
#     with open('/home/hduser/kdd99-analysis/outputs/std.csv') as fin:
#         stds = [float(line[line.find("\t")+len("\t"):line.rfind("\n")]) for line in fin]

    def mapper(self, _, row):
        columns = self.FILE_HEADER.split(self.DELIMITER)
        if row != self.FILE_HEADER:
            values = row.split(self.DELIMITER)
            col_count = len(values)
            
            for i in range(col_count):
                yield (columns[i], float(values[i]))
  
    def reducer(self, key, value):
                       
        values = [v for v in list(value)]
                                           
        def mean(vals):
            return sum(vals)/len(vals)
        
        def skewness(vals):
            m2_e = m3_e = 0
            means = mean(vals)
            N = len(vals)
            
            for v in vals:
                m2_e += (v - means)**2
                m3_e += (v - means)**3
            
            m2 = m2_e / N
            m3 = m3_e / N
            
            if m2 != 0 and m3 != 0:
                
                skewness = m3 / (m2**1.5)
            else:
                skewness = 0.0
                
            return skewness
        
        yield key, skewness(values)
        
if __name__ == '__main__':
     SkewnessMRJob.run()           
