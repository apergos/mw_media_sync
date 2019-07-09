#!/usr/bin/python3
import os
import gzip
import glob
import hashlib
import json
import shutil
import sys
import time
from subprocess import Popen, PIPE
import urllib
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


class Projects():
    '''keeping track of and manipulating the list of projects
    active on the remote end, the list of projects to do, etc.'''

    @staticmethod
    def is_active(project):
        '''if the project name has a '/' in it, it's not active. it means
        we didn't find it in the list of projects on the remote host,
        no dbname, all we have are the projecttype and the langcode and
        we codged together a so-called project name out of that.'''
        return not bool('/' in project)

    @staticmethod
    def get_projecttype_from_url(url):
        '''give an url blah.wikisomething.org, dig the wikisomething
        piece out and return it. yes it stinks.'''
        # https://si.wikipedia.org
        return url.rsplit('.', 2)[1]

    @staticmethod
    def exclude_foreign_repo(config, active_projects):
        '''toss the foreign repo from the list of active
        projects. We won't mirror all that content!
        For Wikimedia project mirroring, the foreign repo
        would be commons.wikimedia.org (commonswiki).'''
        if config['foreignrepo']:
            if config['foreignrepo'] in active_projects:
                active_projects.pop(config['foreignrepo'], None)

    def __init__(self, config, projects_todo, dryrun):
        '''
        args: config
              dict of active projects
              list of projects to do
        '''
        self.config = config
        self.dryrun = dryrun
        self.active = self.get_active_projects()
        self.exclude_foreign_repo(config, self.active)
        self.todo = self.filter_projects_todo(projects_todo)
        self.projecttypes_langcodes_to_dbnames = self.get_active_projects_by_projecttype_langcode()

    def get_active_projects(self):
        '''get list of active projects from remote MediaWiki
        via the api, convert it to a dict of entries with key the dbname
        and containing projecttype, langcode for each wiki, and return it'''
        baseurl = (self.config['api_url'])
        params = {'action': 'sitematrix', 'format': 'json'}
        sess = requests.Session()
        sess.headers.update(
            {"User-Agent": self.config['agent'], "Accept": "application/json"})
        getter = WebGetter(self.config, dryrun=self.dryrun)
        errors = 'Failed to retrieve list of active projects'
        content = getter.get_content(baseurl, errors, session=sess, params=params)

        siteinfo = json.loads(content)
        active_projects = {}
        for sitegroup in siteinfo['sitematrix']:
            if sitegroup == 'specials':
                for site in siteinfo['sitematrix'][sitegroup]:
                    active_projects[site['dbname']] = {
                        'projecttype': self.get_projecttype_from_url(site['url']),
                        'langcode': site['code']}
                continue

            # there is a 'count' entry which doesn't have site info. might be others too.
            try:
                if 'site' in siteinfo['sitematrix'][sitegroup]:
                    # sitegroup is 266:
                    # '266': {'code': 'tk', 'name': 'Türkmençe', 'site':
                    # [{'url': 'https://tk.wikipedia.org', 'dbname': 'tkwiki',
                    #   'code': 'wiki', 'sitename': 'Wikipediýa'},
                    #  {'url': 'https://tk.wiktionary.org', 'dbname': 'tkwiktionary',
                    #   'code': 'wiktionary', 'sitename': 'Wikisözlük'},
                    #  {'url': 'https://tk.wikibooks.org', 'dbname': 'tkwikibooks',
                    #   'code': 'wikibooks', 'sitename': 'Wikibooks', 'closed': ''},
                    #  {'url': 'https://tk.wikiquote.org', 'dbname': 'tkwikiquote',
                    #   'code': 'wikiquote', 'sitename': 'Wikiquote', 'closed': ''}]
                    for site in siteinfo['sitematrix'][sitegroup]['site']:
                        active_projects[site['dbname']] = {
                            'projecttype': self.get_projecttype_from_url(site['url']),
                            'langcode': siteinfo['sitematrix'][sitegroup]['code']}
            except TypeError:
                continue
        return active_projects

    def get_active_projects_by_projecttype_langcode(self):
        '''convert the active projects dict into one that uses the projecttype and langcode
        for a key and spits out the dbname (projectname) instead'''
        to_return = {}
        for dbname in self.active:
            projecttype = self.active[dbname]['projecttype']
            langcode = self.active[dbname]['langcode']
            to_return[projecttype + '/' + langcode] = dbname
        return to_return

    def get_projectname_from_type_langcode(self, projecttype, langcode):
        '''given the project type and the so-called langcode, return
        the project name (i.e. dbname) if it exists in active projects.
        if it does not, return projectype/langcode (caller should use the embedded /
        as an indicator that the project is not known any longer on the remote side)'''
        try:
            return self.projecttypes_langcodes_to_dbnames[projecttype + '/' + langcode]
        except KeyError:
            return projecttype + '/' + langcode

    def get_projecttype_langcode(self, project):
        '''given a project name which is either a dbname (and findable in
        active_projects) or a string of the format projecttype/langcode, return
        the projecttype and the so-called langcode, as in the examples below:
        enwiki -> wikipedia, en
        commonswiki -> wikipedia, commons'''
        if '/' in project:
            projecttype, langcode = project.split('/')
        else:
            projecttype = self.active[project]['projecttype']
            langcode = self.active[project]['langcode']
        return (projecttype, langcode)

    def filter_projects_todo(self, projects_todo):
        '''
        return only projects in todo list that are active
        '''
        filtered = None
        if projects_todo:
            filtered = [project for project in projects_todo
                        if project in self.active]
        return filtered


class LocalFiles():
    '''methods for setting up local directories, listing local media,
    archiving projects locally, etc'''
    def __init__(self, config, projects, verbose=False, dryrun=False):
        '''
        configparser instance,
        Projects instance
        '''
        self.config = config
        self.projects = projects
        self.today = time.strftime("%Y%m%d", time.gmtime())
        self.verbose = verbose
        self.dryrun = dryrun

    def init_hashdirs(self, basedir):
        '''
        create two levels of subdirectories based on how we hash media files
        and store them
        '''
        # all the hashdirs
        hexdigits = '0123456789abcdef'
        hexdigits_split = list(hexdigits)
        for first_digit in hexdigits_split:
            for ending in hexdigits_split:
                subdir = os.path.join(
                    basedir, first_digit, first_digit + ending)
                if not os.path.exists(subdir):
                    if self.dryrun:
                        print("would make directory(ies):", subdir)
                    else:
                        os.makedirs(subdir)

    def init_mediadirs(self):
        '''if there is no local basedir for media, or if
        any active project dir underneath it does not exist,
        create it and the associated hashdirs'''

        basedir = self.config['mediadir']
        projects_todo = self.projects.active.keys()
        if self.projects.todo:
            projects_todo = self.projects.todo
        for project in projects_todo:
            project_dir = os.path.join(basedir,
                                       self.projects.active[project]['projecttype'],
                                       self.projects.active[project]['langcode'])
            self.init_hashdirs(project_dir)

    def archive_inactive_projects(self):
        '''if a local directory references a project that
        is not active, if the directory is empty (the hash dirs
        underneath it are all empty), remove it; otherwise
        tar up the media and move it to the directory
        archive/inactive/projectname-date-media.tar.gz
        return'''
        for project in self.get_projects():
            if project not in self.projects.active and not self.project_is_empty(project):
                self.archive_project(project)

    def archive_project(self, project):
        '''move the project subdir into the archive area,
        adding the current date and time onto the langcode name'''
        if not os.path.exists(self.config['archivedir']):
            os.makedirs(self.config['archivedir'])

        projecttype, langcode = self.projects.active.get_projecttype_langcode(project)
        now = time.strftime("%Y%m%d%H%M%S", time.gmtime())
        # yes this is only good down to the nearest second.
        # we shouldn't be trying to archive multiple copies
        # of a project in the same second anyways
        newname = langcode + '.' + now

        # very possible that archive area will be on a different filesystem
        # from the (probably web-accessible) media sync, don't take any chances
        if self.dryrun:
            print("would move", os.path.join(self.config['mediadir'], projecttype, langcode),
                  "to", os.path.join(self.config['archivedir'], projecttype, newname))
        else:
            shutil.move(os.path.join(self.config['mediadir'], projecttype, langcode),
                        os.path.join(self.config['archivedir'], projecttype, newname))

    def dir_is_empty(self, dirname):
        '''return True if directory has no contents'''
        return not bool(len(os.listdir(os.path.join(self.config['mediadir'], dirname))))

    def get_projects(self):
        '''return list of locally synced projects'''
        projects = []
        projecttypes = os.listdir(self.config['mediadir'])
        for projecttype in projecttypes:
            langcodes = os.listdir(os.path.join(self.config['mediadir'], projecttype))
            for langcode in langcodes:
                projects.append(self.projects.get_projectname_from_type_langcode(
                    projecttype, langcode))
        return projects

    def get_project_dir(self, project):
        '''given a project name which is either a dbname (and findable in
        active.projects) or a string of the format projecttype/langcode, return
        the full path to the local directory of media for the project, whether
        the dir exists or not'''
        (projecttype, langcode) = self.projects.get_projecttype_langcode(project)
        return os.path.join(self.config['mediadir'], projecttype, langcode)

    def project_is_empty(self, project):
        '''return True if the specified project has no media
        stored locally'''
        project_dir = self.get_project_dir(project)
        hexdigits = '0123456789abcdef'
        hexdigits_split = list(hexdigits)
        for first_digit in hexdigits_split:
            for ending in hexdigits_split:
                subdir = os.path.join(project_dir, first_digit, first_digit + ending)
                if os.path.exists(subdir):
                    if not self.dir_is_empty(subdir):
                        return False
        return True

    def iterate_local_mediafiles_for_project(self, project):
        '''return an iterator which will return the full path of each local media file
        for the specified project, in turn'''
        (projecttype, langcode) = self.projects.get_projecttype_langcode(project)
        basedir = os.path.join(self.config['mediadir'], projecttype, langcode)
        for dirpath, _dirnames, filenames in os.walk(basedir):
            for mediafile in filenames:
                yield os.path.join(dirpath, mediafile)

    def sort_local_media_for_project(self, project, date=None):
        '''read a list of all media for a local active project,
        with path: basename, project name, hashdir and ctime, sort
        it by media file title
        each entry in the file looks likt
        01_Me_and_My_Microphone.ogg YYYYMMDDHHMMSS <mediadir>/wikipedia/en/a/a6/
        the sorted file will live in <listsdir>/date/<project>/<project>_local_media_sorted.gz'''
        if not self.projects.is_active(project):
            if self.verbose:
                print("skipping list of local media for", project, "as not active")
            return
        if not date:
            date = self.today
        basedir = os.path.join(self.config['listsdir'], date, project)
        outputpath = os.path.join(basedir, project + '_local_media_sorted.gz')
        inputpath = os.path.join(basedir, project + '_local_media.gz')
        command = "zcat {infile} | LC_ALL=C sort -k 1 -S 70% | gzip > {outfile}".format(
            infile=inputpath, outfile=outputpath)
        if self.dryrun:
            print("for project", project, "would sort media into", outputpath, 'with command:')
            print(command)
        else:
            # these lists can be huge so let's not fool ourselves into thinking
            # we're going to do it all in memory.
            with Popen(command, shell=True, stderr=PIPE) as proc:
                _unused_output, errors = proc.communicate()
                if errors:
                    print(errors.decode('utf-8').rstrip('\n'))

    def record_local_media_for_project(self, project, date=None):
        '''write a list of all media for a local active project,
        with path: basename, project name, hashdir and ctime
        so each entry will look like:
        01_Me_and_My_Microphone.ogg YYYYMMDDHHMMSS <mediadir>/wikipedia/en/a/a6/
        this file will live in <listsdir>/date/<project>/<project>_local_media.gz'''
        if not self.projects.is_active(project):
            if self.verbose:
                print("skipping list of local media for", project, "as not active")
            return
        if not date:
            date = self.today
        basedir = os.path.join(self.config['listsdir'], date, project)
        if not os.path.exists(basedir):
            os.makedirs(basedir)
        # if it's already there, what does that mean for us? we'll overwrite the
        # existing list. too bad. maybe we're redoing a bad run or something.
        outputpath = os.path.join(basedir, project + '_local_media.gz')
        if self.dryrun:
            print("for project", project, "would log media into", outputpath)
        else:
            with gzip.open(outputpath, "wb") as output:
                for path in self.iterate_local_mediafiles_for_project(project):
                    dirname, filename = os.path.split(path)
                    # yep we get to stat them all. groan
                    mtime = os.stat(path).st_mtime
                    timestamp = time.strftime("%Y%m%d%H%M%S", time.gmtime(mtime))
                    output.write('{filename} {timestamp} {dirname}\n'.format(
                        filename=filename, timestamp=timestamp, dirname=dirname).encode('utf-8'))

    def get_local_media_lists(self):
        '''write a list of all media for each local project in the todo list'''
        if not os.path.exists(self.config['listsdir']):
            os.makedirs(self.config['listsdir'])
        local_projects = self.get_projects()
        for project in local_projects:
            if project in self.projects.todo:
                self.record_local_media_for_project(project)

    def sort_local_media_lists(self):
        '''read and sort the local media lists'''
        local_projects = self.get_projects()
        for project in local_projects:
            if project in self.projects.todo:
                self.sort_local_media_for_project(project)


class ListsGetter():
    '''methods to retrieve various lists of media files per project
    from remote location'''
    def __init__(self, config, projects, verbose=False, dryrun=False):
        '''
        configparser instance,
        Projects instance,
        foreign repo info,
        list of projects to actually operate on
        '''
        self.config = config
        self.projects = projects
        self.local = LocalFiles(config, projects, verbose, dryrun)
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
        for project in self.projects.active:
            if project in self.projects.todo:
                filename = filename_template.format(project=project, date=date)
                url = baseurl + '/' + filename
                output_path = os.path.join(self.config['listsdir'],
                                           self.local.today, project, filename)
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


class ListsMaker():
    '''methods to generate lists of media files we need: media to delete,
    to keep, to download'''

    @staticmethod
    def get_filename_time(line):
        '''from a line containing filename<whitespace>time<whitespace> + maybe other stuff,
        return the filename and the timestamp'''
        fields = line.rstrip().split()
        return(fields[0], fields[1])

    @staticmethod
    def get_most_recent_file(project, filename, most_recent_lists):
        '''
        for a specific list file (with project name removed),
        find the date of the most recent version and return it,
        or None if there is no such file
        '''
        if project in most_recent_lists:
            dates = sorted(most_recent_lists[project].keys(), reverse=True)
            for date in dates:
                if filename in most_recent_lists[project][date]:
                    return date
        return None

    def __init__(self, config, projects, verbose=False, dryrun=False):
        '''
        configparser instance,
        Projects instance,
        foreign repo info,
        list of projects to actually operate on
        '''
        self.config = config
        self.projects = projects
        self.projects_todo = None
        self.local = LocalFiles(config, projects, verbose, dryrun)
        self.verbose = verbose
        self.dryrun = dryrun

    def remove_first_line_sort(self, inpath, outpath):
        '''remove first line of contents of gzipped input file, gzip,
        write to output file'''
        zcat_tail_command = "zcat {infile} | tail -n +2 "
        sort_command = " LC_ALL=C sort -k 1 -S 70% "
        uniq_gzip_command = " uniq | gzip > {outfile}"
        command = zcat_tail_command + '|' + sort_command + '|' + uniq_gzip_command
        command = command.format(
            infile=inpath, outfile=outpath)
        if self.dryrun:
            print("would filter/sort media list", inpath, "into", outpath, 'with command:')
            print(command)
        else:
            # these lists can be huge so let's not fool ourselves into thinking
            # we're going to do it all in memory.
            if self.verbose:
                print("about to run command:", command)
            with Popen(command, shell=True, stderr=PIPE) as proc:
                _unused_output, errors = proc.communicate()
                if errors:
                    print(errors.decode('utf-8').rstrip('\n'))

    def cleanup_project_media_lists(self, in_filename_template, out_filename_template):
        '''uncompress, remove the first line which reflects sql table columns,
        sort, write anew in filelists/date/projectname/projectname-uploads-sorted.gz,
        for all projects to do
        in_ and out_ filename templates will have the project name substituted in.
        example filename template: {project}-*-local-wikiqueries.gz'''
        for project in self.projects.active:
            if project in self.projects.todo:
                filename_base = in_filename_template.format(project=project)
                media_lists_path = os.path.join(self.config['listsdir'],
                                                self.local.today, project, filename_base)
                todays_files = glob.glob(media_lists_path)
                if not todays_files:
                    print("warning: no media list files found of format", in_filename_template,
                          "for project", project)
                    continue

                most_recent = sorted(todays_files)[-1]
                if not most_recent.endswith('.gz'):
                    print("warning: bad filename found,", most_recent)
                    continue
                newname = os.path.join(self.config['listsdir'], self.local.today, project,
                                       out_filename_template.format(project=project))

                if self.dryrun:
                    print("would filter {old} to {new}".format(
                        old=most_recent, new=newname))
                    return

                self.remove_first_line_sort(most_recent, newname)

    def cleanup_project_uploaded_media_lists(self):
        '''uncompress, remove the first line which reflects sql table columns,
        sort, write anew in filelists/date/projectname/projectname-uploads-sorted.gz,
        for all projects to do'''
        in_filename_template = '{project}-*-local-wikiqueries.gz'
        out_filename_template = '{project}-uploads-sorted.gz'
        self.cleanup_project_media_lists(in_filename_template, out_filename_template)

    def cleanup_project_foreignrepo_media_lists(self):
        '''uncompress, remove the first line which reflects sql table columns,
        sort, write anew in filelists/date/projectname/projectname-foreignrepo-sorted.gz,
        for all projects todo'''
        in_filename_template = '{project}-*-remote-wikiqueries.gz'
        out_filename_template = '{project}-foreignrepo-sorted.gz'
        self.cleanup_project_media_lists(in_filename_template, out_filename_template)

    def list_uploaded_files_toget_for_project(self, local_files_list,
                                              uploaded_files_list, output_path):
        '''reading the list of uploaded files to get for each active
        project, check the list of local files and see if we have it;
        if we do, see if the timestamp is more recent than the timestamp
        in the uploaded files list. If our copy is older or is missing,
        add this file to the list of files to get from the local project:
        filelists/date/projectname/projectname-uploaded-files-to-get.gz'''
        local_file = b""
        if self.dryrun:
            print("would write {outpath} from {localpath}, {uploadedpath}".format(
                outpath=output_path, localpath=local_files_list, uploadedpath=uploaded_files_list))
            return
        if self.verbose:
            print("writing {outpath} from {localpath}, {uploadedpath}".format(
                outpath=output_path, localpath=local_files_list, uploadedpath=uploaded_files_list))
        with gzip.open(local_files_list, "rb") as local_files:
            with gzip.open(uploaded_files_list, "rb") as uploaded_files:
                with gzip.open(output_path, "wb") as output:
                    while True:
                        uploaded_line = uploaded_files.readline()
                        if not uploaded_line:
                            # done!
                            return
                        uploaded_file, uploaded_time = self.get_filename_time(uploaded_line)
                        while (local_file is not None and local_file < uploaded_file):
                            last_local_line = local_files.readline().rstrip()
                            if last_local_line:
                                local_file, local_time = self.get_filename_time(last_local_line)
                            else:
                                local_file = None
                        if not local_file or local_file > uploaded_file:
                            # we don't have it. we want it.
                            output.write(uploaded_file + b'\n')
                        elif local_file == uploaded_file and local_time < uploaded_time:
                            # our timestamp is older than remote upload's
                            output.write(uploaded_file + b'\n')

    def generate_uploaded_files_to_get(self):
        '''for each project to do, generate a list of uploaded files we
        don't have locally, and write the list into a file for retrieval later.'''
        basedir = self.config['listsdir']
        for project in self.projects.active:
            if project in self.projects.todo:
                local_files_list = os.path.join(basedir, self.local.today, project,
                                                project + '_local_media_sorted.gz')
                uploaded_files_list = os.path.join(basedir, self.local.today, project,
                                                   project + '-uploads-sorted.gz')
                output_path = os.path.join(basedir, self.local.today, project,
                                           project + '-uploaded-toget.gz')
                self.list_uploaded_files_toget_for_project(
                    local_files_list, uploaded_files_list, output_path)

    def list_foreignrepo_files_toget_for_project(self, local_files_list,
                                                 foreignrepo_files_list, output_path):
        '''reading the list of foreign repo files to get for each active
        project, check the list of local files and see if we have it;
        if we don't, add this file to the list of files to get from the
        local project:
        filelists/date/projectname/projectname-foreignrepo-files-to-get.gz
        '''
        localfile = b""
        if self.dryrun:
            print("would write {outpath} from {localpath}, {foreignrepopath}".format(
                outpath=output_path, localpath=local_files_list,
                foreignrepopath=foreignrepo_files_list))
            return
        if self.verbose:
            print("writing {outpath} from {localpath}, {foreignrepopath}".format(
                outpath=output_path, localpath=local_files_list,
                foreignrepopath=foreignrepo_files_list))
        with gzip.open(local_files_list, "rb") as local_files:
            with gzip.open(foreignrepo_files_list, "rb") as foreignrepo_files:
                with gzip.open(output_path, "wb") as output:
                    while True:
                        foreignrepo_line = foreignrepo_files.readline()
                        if not foreignrepo_line:
                            # done!
                            return
                        foreignrepofile = foreignrepo_line.rstrip()
                        while (localfile is not None and localfile < foreignrepofile):
                            last_local_line = local_files.readline()
                            if last_local_line:
                                localfile = last_local_line.split()[0]
                            else:
                                localfile = None
                        if not localfile or localfile > foreignrepofile:
                            # we don't have it. we want it.
                            output.write(foreignrepofile + b'\n')

    def generate_foreignrepo_files_to_get(self):
        '''reading the list of foreignrepo files to get for each active
        project, check the list of local files and see if we have it.
        If our copy is missing, add this file to the list of files to
        get from the foreign repo:
        filelists/date/projectname/projectname-foreignrepo-files-to-get.gz
        NOTE that we might have an older copy than the remote server
        and we have no way to check for that at present. FIXME'''
        basedir = self.config['listsdir']
        for project in self.projects.active:
            if project in self.projects.todo:
                local_files_list = os.path.join(basedir, self.local.today, project,
                                                project + '_local_media_sorted.gz')
                foreignrepo_files_list = os.path.join(basedir, self.local.today, project,
                                                      project + '-foreignrepo-sorted.gz')
                output_path = os.path.join(basedir, self.local.today, project,
                                           project + '-foreignrepo-toget.gz')
                self.list_foreignrepo_files_toget_for_project(
                    local_files_list, foreignrepo_files_list, output_path)

    def merge_media_files_to_keep(self):
        '''for each active project, merge the lists of uploaded
        and foreignrepo media to keep, into one sorted list,
        which can later be compared with the sorted local list
        of media we have for the project, so that we can delete anything
        not on the remote server.'''
        # we have two lists, <project>-uploads-sorted.gz which should contain
        # everything project-uploaded we have or ought to have, and
        # <project>-foreignrepo-sorted.gz which should contain everything
        # foreign-repo-uploaded we have or ought to have for this project.
        # SO, we should just be able to merge them into one file, which can
        # be used to figure out what to delete.
        # NOTE THAT the uploaded list has filename<whitespace>timestamp
        # while the other one just has filename
        # So when we use this file later we need to bear that in mind
        for project in self.projects.active:
            if project in self.projects.todo:
                basedir = os.path.join(self.config['listsdir'], self.local.today, project)
                uploaded = os.path.join(basedir, project + '-uploads-sorted.gz')
                foreign = os.path.join(basedir, project + '-foreignrepo-sorted.gz')
                outpath = os.path.join(basedir, project + '-all-media-keep.gz')
                sort_command = "LC_ALL=C sort -m "
                gunzip_commands = "<(gunzip -c {uploaded}) <(gunzip -c {foreign})".format(
                    uploaded=uploaded, foreign=foreign)
                command = sort_command + gunzip_commands + " | gzip > " + outpath
                if self.dryrun:
                    print("for project", project, "would merge media-to-keep into",
                          outpath, 'with command:')
                    print(command)
                else:
                    with Popen(command, shell=True, stderr=PIPE, executable='/bin/bash') as proc:
                        _unused_output, errors = proc.communicate()
                        if errors:
                            print(errors.decode('utf-8').rstrip('\n'))
                            return

    def list_local_media_not_on_remote(self):
        '''for each project to do, list all media not on the remote
        server (either as project upload or in foreign repo and used
        by the project)'''
        for project in self.projects.active:
            if project in self.projects.todo:
                basedir = os.path.join(self.config['listsdir'], self.local.today, project)
                keeps_list = os.path.join(basedir, project + '-all-media-keep.gz')
                haves_list = os.path.join(basedir, project + '_local_media_sorted.gz')
                deletes_list = os.path.join(basedir, project + '-all-media-delete.gz')
                if self.dryrun:
                    print("would write {deletes} from {keeps}, {haves}".format(
                        deletes=deletes_list, keeps=keeps_list, haves=haves_list))
                    return
                if self.verbose:
                    print("writing {deletes} from {keeps}, {haves}".format(
                        deletes=deletes_list, keeps=keeps_list, haves=haves_list))
                with gzip.open(keeps_list, "rb") as keeps:
                    with gzip.open(haves_list, "rb") as haves:
                        with gzip.open(deletes_list, "wb") as deletes:
                            keep = None
                            keeps_eof = False
                            while True:
                                have_line = haves.readline()
                                if not have_line:
                                    # done
                                    return
                                have = have_line.split()[0]
                                while (keep is None or keep < have) and not keeps_eof:
                                    keep_line = keeps.readline()
                                    if not keep_line:
                                        keeps_eof = True
                                        break
                                    # some entries in here look like
                                    # LettertoDefenceMinister.pdf	20070115045609
                                    # because they are from a query against the image table
                                    keep = keep_line.split()[0]
                                if keeps_eof or keep > have:
                                    # not in the keep list. delete!
                                    deletes.write(have_line)

    def get_most_recent(self):
        '''
        for all lists,
        find the most recent date each file was produced for each project,
        excluding today's run,
        creating a dict of projects vs dates, and returning the dict,
        or None if no list files have never been written
        '''
        list_date_info = {}
        date = None
        basedir = self.config['listsdir']
        dates = os.listdir(basedir)
        dates = [date for date in dates if len(date) == 8 and date.isdigit()]
        for date in dates:
            if date == self.local.today:
                continue
            projects = os.listdir(os.path.join(basedir, date))
            for project in projects:
                if project not in list_date_info:
                    list_date_info[project] = {}
                list_date_info[project][date] = []
                files = os.listdir(os.path.join(basedir, date, project))
                list_date_info[project][date].extend(
                    [filename[len(project):] for filename in files])
        return list_date_info

    def diff_lists(self, project, date, in_suffix, out_suffix):
        '''
        for a given project and date, find the list of media
        (<project> + in_suffix) for that project, generated on that date,
        and write out all entries in that file not in today's list
        for the same project.
        '''
        oldfile = os.path.join(self.config['listsdir'], date, project,
                               project + in_suffix)
        todayfile = os.path.join(self.config['listsdir'], self.local.today, project,
                                 project + in_suffix)
        outfile = os.path.join(self.config['listsdir'], self.local.today, project,
                               project + out_suffix)
        if self.dryrun:
            print("would write {out} from {old}, {today}".format(
                out=outfile, old=oldfile, today=todayfile))
            return
        if self.verbose:
            print("writing {out} from {old}, {today}".format(
                out=outfile, old=oldfile, today=todayfile))
        with gzip.open(oldfile, "rb") as old_in:
            with gzip.open(todayfile, "rb") as today_in:
                with gzip.open(outfile, "wb") as output:
                    newline = None
                    while True:
                        oldline = old_in.read()
                        if not oldline:
                            return
                        while newline is None or newline < oldline:
                            newline = today_in.read()
                        if newline is None or newline > oldline:
                            output.write(newline)

    def list_media_gone_from_remote(self, most_recent_lists):
        '''
        for each project to do,
        if there is a previous list of <project>-all-media-keep.gz, compare the current
        list to the most recent such list, and generate a list of media that is no longer
        on the remote, and therefore should be deleted by us.
        this can be called instead of generating a local list of all media and figuring
        out deletes from that, in the case there is a previous remotes media list. It
        will save a lot of stat calls.
        '''
        for project in self.projects.active:
            if project in self.projects.todo:
                date = self.get_most_recent_file(project, '-all-media-keep.gz',
                                                 most_recent_lists)
                if  date:
                    self.diff_lists(project, date, '-all-media-keep.gz', '-all-media-gone.gz')

    def list_new_uploaded_media_on_remote(self, most_recent_lists):
        '''
        for each project to do,
        if there is a previous list of <project>-uploads-sorted.gz, compare the current
        list to the most recent such list, and generate a list of media that is new on
        the remote, uploaded to the project, and therefore should be downloaded by us.
        this can be called instead of generating a local list of all media and figuring
        out downloads from that, in the case there is a previous remotes media list. It
        will save a lot of stat calls.
        '''
        return

    def list_new_foreign_media_on_remote(self, most_recent_lists):
        '''
        for each project to do,
        if there is a previous list of <project>-foreignrepo-sorted.gz, compare the current
        list to the most recent such list, and generate a list of media that is new on
        the remote, uploaded to the foreign repo, and therefore should be downloaded by us.
        this can be called instead of generating a local list of all media and figuring
        out downloads from that, in the case there is a previous remotes media list. It
        will save a lot of stat calls.
        '''
        return


class Sync():
    '''methods for syncing media from a remote MediaWiki
    instance or instances, to a local server'''
    EXTS = ['ai', 'aif', 'aiff', 'avi', 'dia', 'djvu', 'doc', 'dv',
            'eps', 'gif', 'indd', 'inx', 'jpg', 'jpeg', 'mid', 'mov',
            'odg', 'odp', 'ods', 'odt', 'ogg', 'ogv', 'omniplan', 'otf', 'ott',
            'pdf', 'png', 'ppd', 'ppt', 'psd', 'stl', 'svg',
            'wff2', 'webp', 'wmv', 'woff', 'xcf', 'xml', 'zip']

    @staticmethod
    def is_sane_mediafilename(filename):
        '''because people can literally force any random string to appear
        in the global image links table but using it in a gallery, let's
        filter out the obvious cruft, make sure that the file has a known
        good extension, etc.'''
        good = False
        to_check = filename.decode('utf-8')
        if '/' in to_check or os.path.sep in to_check:
            # fast fail
            return False
        for ext in Sync.EXTS:
            if to_check.endswith('.' + ext):
                good = True
                break
        if not good:
            # no good ext
            return False
        # FIXME more sanity checks?
        return True

    @staticmethod
    def get_hashpath(filename, depth):
        '''given a filename get the hashpath (x/yy(/zzz, etc)) for media storage
        for mediawiki hashes 'depth' directories deep'''
        summer = hashlib.md5()
        summer.update(filename)
        md5_hash = summer.hexdigest()
        path = ''
        for i in range(1, depth+1):
            path = path + md5_hash[0:i] + '/'
        return path.rstrip('/')

    def __init__(self, config, projects, verbose=False, dryrun=False):
        '''
        configparser instance,
        Projects instance,
        foreign repo info,
        list of projects to actually operate on
        '''
        self.config = config
        self.projects = projects
        self.projects_todo = None
        self.local = LocalFiles(config, projects, verbose, dryrun)
        self.verbose = verbose
        self.dryrun = dryrun

    def delete_local_media_not_on_remote(self):
        '''for each project todo, 'delete' all media not on the remote
        server (either as project upload or in foreign repo and used
        by the project)
        'deleted' media will be moved to
        archive/deleted/projecttype/langcode/hashdir/imagename
        if there is already an file in that location, it will be
        overwritten, we do not keep more than one deleted version
        around
        folks who want to permanently remove such images can
        periodically clean out the archive/deleted directory'''
        for project in self.projects.active:
            if project in self.projects.todo:
                basedir = os.path.join(self.config['listsdir'], self.local.today, project)
                deletes_list = os.path.join(basedir, project + '-all-media-delete.gz')
                (projecttype, langcode) = self.local.projects.get_projecttype_langcode(project)
                archived_deletes_dir = os.path.join(self.config['archivedir'], 'deleted',
                                                    projecttype, langcode)
                if self.dryrun:
                    print("would move entries in {deletes} to {archived} ".format(
                        deletes=deletes_list, archived=archived_deletes_dir))
                    return
                if not os.path.exists(archived_deletes_dir):
                    os.makedirs(archived_deletes_dir)
                    self.local.init_hashdirs(archived_deletes_dir)
                if self.verbose:
                    print("moving entries in {deletes} to {archived} ".format(
                        deletes=deletes_list, archived=archived_deletes_dir))
                with gzip.open(deletes_list, "rb") as deletes:
                    while True:
                        delete_line = deletes.readline()
                        if not delete_line:
                            # eof
                            return
                        filename = delete_line.rstrip().split()[0]
                        hashpath = self.get_hashpath(filename, 2)
                        old_path = os.path.join(self.config['mediadir'], projecttype, langcode,
                                                hashpath, filename.decode('utf-8'))
                        new_path = os.path.join(archived_deletes_dir,
                                                hashpath, filename.decode('utf-8'))
                        shutil.move(old_path, new_path)

    def get_media_download_url(self, file_toget, project, hashpath, upload_type):
        '''get and return the url for downloading the original media
        upload_type is local or foreignrepo'''
        # https://upload.wikimedia.org/projecttype/langcode/hash/dir/filename

        # deal with silly things like % and other fun characters in the url
        encoded_toget = urllib.parse.quote(file_toget.decode('utf-8'))

        if upload_type == 'local':
            (projecttype, langcode) = self.projects.get_projecttype_langcode(project)
            projecturl = '{baseurl}/{ptype}/{lcode}'.format(
                baseurl=self.config['uploaded_media_url'],
                ptype=projecttype, lcode=langcode)
        elif upload_type == 'foreignrepo':
            projecturl = self.config['foreignrepo_media_url']
        else:
            # we have no idea what the caller wants
            return None

        url = '{projecturl}/{hashdirs}/{toget}'.format(
            projecturl=projecturl, hashdirs=hashpath, toget=encoded_toget)
        return url

    def get_new_media_for_project(self, repotype, project, maxgets, fhandles):
        '''
        given file handle to list of files to retrieve,
        file handles to where to log successful retrievals
        and failures, get all the files, logging the results,
        storing them appropriately
        '''
        getter = WebGetter(self.config, self.dryrun)
        (projecttype, langcode) = self.projects.get_projecttype_langcode(project)
        download_basedir = os.path.join(self.config['mediadir'], projecttype, langcode)
        gets = 0
        while gets < maxgets:
            gets += 1

            toget = fhandles['toget_in'].readline()
            if not toget:
                # end of file
                return
            toget = toget.rstrip()
            if not self.is_sane_mediafilename(toget):
                continue

            hashpath = self.get_hashpath(toget, 2)
            url = self.get_media_download_url(toget, project, hashpath, repotype)

            resp_code = getter.get_file(url,
                                        os.path.join(download_basedir, hashpath,
                                                     toget.decode('utf-8')),
                                        'failed to download media on ' + project + ' via ' + url,
                                        return_on_fail=True)
            if resp_code:
                fhandles['fail_out'].write("'{filename}' [{code}] {url}\n".format(
                    filename=toget, url=url, code=resp_code).encode('utf-8'))
            else:
                fhandles['retr_out'].write("'{filename}' {url}".format(
                    filename=toget, url=url).encode('utf-8'))
            time.sleep(self.config['http_wait'])

    def get_new_media_from_list(self, max_gets, repotype, files):
        '''for each project todo, get uploaded media that
        we don't have locally, up to some number (configured)
        of items
        write all entries we retrieved to
        filelists/date/projectname/projectname_local_retrieved.gz
        write all entries we failed to retrieve after n retries, to
        filelists/date/projectname/projectname_local_get_failed.gz
        The point of limiting the number of retrievals is to
        do only so many at a time, before the next deletion run,
        in case there's been a long gap between runs and you're
        playing catch-up.'''
        fhandles = {}
        for project in self.projects.active:
            if project in self.projects.todo:
                if self.dryrun:
                    print("would get files from {flist}, logs {failed} (failed), {ok} (ok)".format(
                        flist=files['toget'], failed=files['failed'], ok=files['retrieved']))
                    return
                with gzip.open(files['toget'], "rb") as fhandles['toget_in']:
                    with gzip.open(files['retrieved'], "wb") as fhandles['retr_out']:
                        with gzip.open(files['failed'], "wb") as fhandles['fail_out']:
                            self.get_new_media_for_project(repotype, project, max_gets, fhandles)

    def get_new_media(self):
        '''
        for each project todo, get uploaded and foreign
        media that we don't have locally, up to some number (configured)
        all successfull gets and all failures are logged to
        (gzipped) files for reference
        '''
        basedir = self.config['listsdir']
        max_local_gets = self.config['max_uploaded_gets']
        max_foreignrepo_gets = self.config['max_foreignrepo_gets']
        files = {}
        for project in self.projects.active:
            if project in self.projects.todo:
                files['retrieved'] = os.path.join(basedir, self.local.today, project,
                                                  project + '_local_retrieved.gz')
                files['failed'] = os.path.join(basedir, self.local.today, project,
                                               project + '_local_get_failed.gz')
                files['toget'] = os.path.join(basedir, self.local.today, project,
                                              project + '-uploaded-toget.gz')
                self.get_new_media_from_list(max_local_gets, 'local', files)
                files['retrieved'] = os.path.join(basedir, self.local.today, project,
                                                  project + '_foreignrepo-retrieved.gz')
                files['failed'] = os.path.join(basedir, self.local.today, project,
                                               project + '_foreignrepo_get_failed.gz')
                files['toget'] = os.path.join(basedir, self.local.today, project,
                                              project + '-foreignrepo-toget.gz')
                self.get_new_media_from_list(max_foreignrepo_gets, 'foreignrepo', files)
