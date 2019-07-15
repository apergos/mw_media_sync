#!/usr/bin/python3
import os
from sync.webgetter import WebGetter


class ListsGetter():
    '''methods to retrieve various lists of media files per project
    from remote location'''
    def __init__(self, config, projects, today, verbose=False, dryrun=False):
        '''
        configparser instance,
        Projects instance,
        foreign repo info,
        list of projects to actually operate on
        '''
        self.config = config
        self.projects = projects
        self.today = today
        self.verbose = verbose
        self.dryrun = dryrun

    def get_latest_uploaded_medialists_date(self):
        '''find the most recent date for lists uploaded media per project
        at the specified url in the config'''
        baseurl = self.config['media_filelists_url']
        getter = WebGetter(self.config, self.dryrun)
        errors = 'Failed to retrieve list of dates of uploaded media'
        content = getter.get_content(baseurl, errors)
        # entries we want:
        # <a href="20190210/">20190210/</a>                                  10-Feb-2019 11:45
        content = content.decode('utf-8')
        dates = [line.split('"')[1].rstrip('/') for line in content.splitlines()
                 if '<a href=' in line]
        dates = [date for date in dates if date.isdigit() and len(date) == 8]
        if dates:
            latest = sorted(dates)[-1]
            if self.verbose:
                print("Latest remote media file lists for", baseurl, "have date", latest)
            return latest

        if self.verbose:
            print("No uploaded media files for", baseurl)
        return None

    def get_project_remote_media(self, filename_template, err_message):
        '''get via http from remote server the latest list of
        filenames/timestamps of media with the given filename
        template, plugging in project name and date, for all projects to do
        Example filename template: {project}-{date}-local-wikiqueries.gz'''
        date = self.get_latest_uploaded_medialists_date()
        if not date:
            return

        baseurl = self.config['media_filelists_url'] + '/' + date
        getter = WebGetter(self.config, self.dryrun)
        todos = self.projects.get_todos()
        for project in todos:
            filename = filename_template.format(project=project, date=date)
            url = baseurl + '/' + filename
            output_path = os.path.join(self.config['listsdir'],
                                       self.today, project, filename)
            getter.get_file(url, output_path, err_message + project)

    def get_project_uploaded_media(self):
        '''get via http from remote server the latest list
        of filenames/timestamps of media that was uploaded locally
        to each project, for projects to do'''
        error = 'Failed to retrieve list of uploaded media for project: '
        self.get_project_remote_media('{project}-{date}-local-wikiqueries.gz', error)

    def get_project_foreignrepo_media(self):
        '''get via http from remote server the latest list
        of names of media that were uploaded to the remote
        repo for a project but are used locally, for projects
        to do'''
        error = 'Failed to retrieve list of foreign repo media for project: '
        self.get_project_remote_media('{project}-{date}-remote-wikiqueries.gz', error)
