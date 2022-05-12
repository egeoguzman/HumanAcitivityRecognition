#!/usr/bin/env python3
# PearsonCorrelationMRJob.py

from mrjob.job import MRJob

class CovvarianceMatrixMRJob(MRJob):
    DELIMITER = ","
    variable_names = []

    def configure_args(self):
        super(CovvarianceMatrixMRJob, self).configure_args()
        self.add_file_arg("--variables",type=str)

    def mapper(self, _, row):
        columns = row.split(self.DELIMITER)
        col_count = len(columns)
        n = 1
        # output each pair combination of columns
        for i in range(col_count):
            for j in range(col_count):
                x = float(columns[i])
                y = float(columns[j])
                xsq = x ** 2
                ysq = y ** 2
                xy = x * y
                yield str(i) + '-' + str(j), (x, y, xsq, ysq, xy, n)

    def combiner(self, key, values):
        x = y = xsq = ysq = xy = 0.0
        n = 0
        for (xin, yin, xsqin, ysqin, xyin, nin) in values:
            x += xin
            y += yin
            xsq += xsqin
            ysq += ysqin
            xy += xyin
            n += nin
        yield key, (x, y, xsq, ysq, xy, n)

    def reducer(self, key, values):

        n = 0
        x = y = xsq = ysq = xy = 0.0

        for (xin, yin, xsqin, ysqin, xyin, nin) in values:
            x += xin
            y += yin
            xsq += xsqin
            ysq += ysqin
            xy += xyin
            n += nin

        numerator = xy - ((x * y) / n)
        #denominator_l = xsq - ((x ** 2) / n)
        #denominator_r = ysq - ((y ** 2) / n)
        #denominator = (denominator_l * denominator_r) ** 0.5
        denominator = n
        yield key, numerator / denominator

if __name__ == '__main__':
    CovvarianceMatrixMRJob.run()