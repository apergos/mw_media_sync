#!/usr/bin/python3
import os
import gzip
import shutil
import sys
import time
from subprocess import Popen, PIPE
import requests


class WebGetter():
    '''methods for getting content over http(s) and
possibly storing it some nice place, with retries'''
    def __init__(self, config, dryrun):
        self.config = config
        self.dryrun = dryrun

    def get_file(self, url, localpath, err_message):
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
            sys.stderr.write(err_message + ' (response code: {code}\n'.format(
                code=response.status_code))
            response.raise_for_status()
        if self.dryrun:
            print("would save output from", url, "to", localpath)
            return
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


class ActiveProjects():
    '''keeping track of and manipulating the list of projects
    active on the remote end'''

    def __init__(self, active_projects):
        '''
        arg: dict of active projects
        '''
        self.projects = active_projects
        self.projecttypes_langcodes_to_dbnames = self.get_active_projects_by_projecttype_langcode()

    def get_active_projects_by_projecttype_langcode(self):
        '''convert the active projects dict into one that uses the projecttype and langcode
        for a key and spits out the dbname (projectname) instead'''
        to_return = {}
        for dbname in self.projects:
            projecttype = self.projects[dbname]['projecttype']
            langcode = self.projects[dbname]['langcode']
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
            projecttype = self.projects[project]['projecttype']
            langcode = self.projects[project]['langcode']
        return (projecttype, langcode)

    def active(self, project):
        '''if the project name has a '/' in it, it's not active. it means
        we didn't find it in the list of projects on the remote host,
        no dbname, all we have are the projecttype and the langcode and
        we codged together a so-called project name out of that.'''
        return not bool('/' in project)


class LocalFiles():
    '''methods for setting up local directories, listing local media,
    archiving projects locally, etc'''
    def __init__(self, config, active_projects, projects_todo, verbose=False, dryrun=False):
        '''
        configparser instance,
        dict of active projects,
        list of projects to actually operate on
        '''
        self.config = config
        self.active = ActiveProjects(active_projects)
        self.projects_todo = None
        if projects_todo:
            self.projects_todo = [project for project in projects_todo
                                  if project in active_projects]
        self.today = time.strftime("%Y%m%d", time.gmtime())
        self.verbose = verbose
        self.dryrun = dryrun

    def init_mediadirs(self):
        '''if there is no local basedir for media, or if
        any active project dir underneath it does not exist,
        create it and the associated hashdirs'''

        basedir = self.config['mediadir']
        projects_todo = self.active.projects.keys()
        if self.projects_todo:
            projects_todo = self.projects_todo

        for project in projects_todo:
            # all the hashdirs
            hexdigits = '0123456789abcdef'
            hexdigits_split = list(hexdigits)
            for first_digit in hexdigits_split:
                for ending in hexdigits_split:
                    subdir = os.path.join(basedir,
                                          self.active.projects[project]['projecttype'],
                                          self.active.projects[project]['langcode'],
                                          first_digit, first_digit + ending)
                    if not os.path.exists(subdir):
                        if self.dryrun:
                            print("would make directory(ies):", subdir)
                        else:
                            os.makedirs(subdir)

    def archive_inactive_projects(self):
        '''if a local directory references a project that
        is not active, if the directory is empty (the hash dirs
        underneath it are all empty), remove it; otherwise
        tar up the media and move it to the directory
        archive/inactive/projectname-date-media.tar.gz
        return'''
        for project in self.get_projects():
            if project not in self.active.projects and not self.project_is_empty(project):
                self.archive_project(project)

    def archive_project(self, project):
        '''move the project subdir into the archive area,
        adding the current date and time onto the langcode name'''
        if not os.path.exists(self.config['archivedir']):
            os.makedirs(self.config['archivedir'])

        projecttype, langcode = self.active.get_projecttype_langcode(project)
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
                projects.append(self.active.get_projectname_from_type_langcode(
                    projecttype, langcode))
        return projects

    def get_project_dir(self, project):
        '''given a project name which is either a dbname (and findable in
        active.projects) or a string of the format projecttype/langcode, return
        the full path to the local directory of media for the project, whether
        the dir exists or not'''
        (projecttype, langcode) = self.active.get_projecttype_langcode(project)
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
        (projecttype, langcode) = self.active.get_projecttype_langcode(project)
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
        if not self.active.active(project):
            if self.verbose:
                print("skipping list of local media for", project, "as not active")
            return
        if not date:
            date = self.today
        basedir = os.path.join(self.config['listsdir'], date, project)
        outputpath = os.path.join(basedir, project + '_local_media_sorted.gz')
        inputpath = os.path.join(basedir, project + '_local_media.gz')
        command = "zcat {infile} | sort -k 1 -S 70% | gzip > {outfile}".format(
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
        if not self.active.active(project):
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
            if project in self.projects_todo:
                self.record_local_media_for_project(project)

    def sort_local_media_lists(self):
        '''read and sort the local media lists'''
        local_projects = self.get_projects()
        for project in local_projects:
            if project in self.projects_todo:
                self.sort_local_media_for_project(project)


class Sync():
    '''methods for syncing media from a remote MediaWiki
    instance or instances, to a local server'''

    def __init__(self, config, active_projects, projects_todo, verbose=False, dryrun=False):
        '''
        configparser instance,
        dict of active projects,
        list of projects to actually operate on
        '''
        self.config = config
        self.active = ActiveProjects(active_projects)
        self.projects_todo = None
        if projects_todo:
            self.projects_todo = [project for project in projects_todo
                                  if project in active_projects]
        self.local = LocalFiles(config, active_projects, projects_todo, verbose, dryrun)
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
                print("Latest uploaded media files for", baseurl, "have date", latest)
            return latest

        if self.verbose:
            print("No uploaded media files for", baseurl)
        return None

    def get_project_uploaded_media(self):
        '''get via http from remote server the latest list
        of filenames/timestamps of media that was uploaded locally
        to each project, for all active projects'''
        date = self.get_latest_uploaded_medialists_date()
        if not date:
            return

        baseurl = self.config['media_filelists_url'] + '/' + date
        getter = WebGetter(self.config, self.dryrun)
        for project in self.active.projects:
            if project in self.projects_todo:
                filename = '{project}-{date}-local-wikiqueries.gz'.format(
                    project=project, date=date)
                url = baseurl + '/' + filename
                output_path = os.path.join(self.config['listsdir'], self.local.today, filename)
                error = 'Failed to retrieve list of uploaded media for project: ' + project
                getter.get_file(url, output_path, error)

    def get_project_foreignrepo_media(self):
        '''get via http from remote server the latest list
        of names of media that were uploaded to the remote
        repo for a project but are used locally, for all
        active projects'''
        return

    def cleanup_project_uploaded_media_lists(self):
        '''uncompress, remove the first line which reflects sql table columns,
        sort, write anew in filelists/date/projectname/projectname-uploads-sorted.gz,
        for all active projects'''
        return

    def cleanup_project_foreignrepo_media_lists(self):
        '''uncompress, remove the first line which reflects sql table columns,
        sort, write anew in filelists/date/projectname/projectname-foreignrepo-sorted.gz,
        for all active projects'''
        return

    def generate_uploaded_files_to_get(self):
        '''reading the list of uploaded files to get for each active
        project, check the list of local files and see if we have it;
        if we do, see if the timestamp is more recent than the timestamp
        in the uploaded files list. If our copy is older or is missing,
        add this file to the list of files to get from the local project:
        filelists/date/projectname/projectname-uploaded-files-to-get.gz
        Also add all files to
        filelists/date/projectname/projectname-uploaded-files-to-keep,
        we'll use that to figure out deletions later'''
        return

    def generate_foreignrepo_files_to_get(self):
        '''reading the list of foreignrepo files to get for each active
        project, check the list of local files and see if we have it.
        If our copy is missing, add this file to the list of files to
        get from the foreign repo:
        filelists/date/projectname/projectname-foreignrepo-files-to-get.gz
        Also add all files to
        filelists/date/projectname/projectname-foreign-files-to-keep,
        we'll use that to figure out deletions later
        NOTE that we might have an older copy than the remote server
        and we have no way to check for that at present. FIXME'''
        return

    def merge_sort_files_to_keep(self):
        '''for each active project, merge the lists of uploaded
        and foreignrepo media to keep, into one sorted list,
        which can later be compared with the sorted local list
        of media we have for the project, so that we can delete anything
        not on the remote server.'''
        return

    def delete_local_media_not_on_remote(self):
        '''for each active project, 'delete' all media not on the remote
        server (either as project upload or in foreign repo and used
        by the project)
        'deleted' media will be moved to
        archive/deleted/projecttype/langcode/hashdir/imagename
        if there is already an file in that location, it will be
        overwritten, we do not keep more than one deleted version
        around'''
        return

    def get_new_media(self):
        '''for each active project, get uploaded media that
        we don't have locally, up to some number (configured)
        of items; then get foreign repo media for that project
        we don't have locally, again up to some number (configured).
        write all entries we retrieved to
        filelists/date/projectname/projectname_retrieved
        write all entries we failed to retrieve after n retries, to
        filelists/date/projectname/projectname_get_failed
        The point of limiting the number of retrievals is to
        do only so many at a time, before the next deletion run,
        in case there's been a long gap between runs and you're
        playing catch-up.'''
        return
