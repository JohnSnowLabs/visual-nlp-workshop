# evaluatenotebookruns.py
import unittest
import json
import glob
import os
import logging

class TestJobOutput(unittest.TestCase):

    test_output_path = '#ENV#'

    # def test_performance(self):
    #     path = self.test_output_path
    #     statuses = []
    #
    #     for filename in glob.glob(os.path.join(path, '*.json')):
    #         print('Evaluating: ' + filename)
    #         data = json.load(open(filename))
    #         duration = data['execution_duration']
    #         if duration > 100000:
    #             status = 'FAILED'
    #         else:
    #             status = 'SUCCESS'
    #
    #         statuses.append(status)
    #
    #     self.assertFalse('FAILED' in statuses)


    def test_job_run(self):
        path = self.test_output_path
        statuses = []


        for filename in glob.glob(os.path.join(path, '*.json')):
            logging.info('Evaluating: ' + filename)
            print('Evaluating: ' + filename)
            data = json.load(open(filename))
            print(data)
            if data['state']['life_cycle_state'] == "RUNNING":
                statuses.append('NOT_COMPLETED')
            else:
                status = data['state']['result_state']
                statuses.append(status)

        self.assertFalse('FAILED' in statuses)
        self.assertFalse('NOT_COMPLETED' in statuses)

if __name__ == '__main__':
    unittest.main()