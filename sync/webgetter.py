#!/usr/bin/python3
import shutil
import sys
import time
import requests


class WebGetter():
    '''methods for getting content over http(s) and
possibly storing it some nice place, with retries'''
    def __init__(self, config, dryrun):
        self.config = config
        self.dryrun = dryrun

    def get_file(self, url, localpath, err_message, return_on_fail=False):
        '''retrieve a file and put it in the right place,
        with retries and wait between retries as needed'''
        headers = {"User-Agent": self.config['agent']}
        retried = 0
        done = False
        while not done and retried < self.config['http_retries']:
            response = requests.get(url, timeout=5, stream=True, headers=headers)
            if response.status_code == 200:
                done = True
            else:
                retried += 1
                time.sleep(self.config['http_wait'])
        if not done:
            sys.stderr.write(err_message + ' (response code: {code})\n'.format(
                code=response.status_code))
            if return_on_fail:
                return response.status_code
            response.raise_for_status()

        if self.dryrun:
            print("would save output from", url, "to", localpath)
            return 200
        with open(localpath, 'wb') as output:
            shutil.copyfileobj(response.raw, output)

    def get_content(self, url, err_message, params=None, session=None):
        '''retrieve content of a url, with retries'''
        if not session:
            session = requests.Session()
        session.headers.update(
            {"User-Agent": self.config['agent']})
        retried = 0
        done = False
        while not done and retried < self.config['http_retries']:
            response = session.get(url, timeout=5, params=params)
            if response.status_code == 200:
                done = True
            else:
                retried += 1
                time.sleep(self.config['http_wait'])
        if not done:
            sys.stderr.write(err_message +
                             ' (response code: {code}\n'.format(code=response.status_code))
            response.raise_for_status()
        return response.content
