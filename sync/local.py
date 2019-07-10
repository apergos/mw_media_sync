#!/usr/bin/python3
import os
import gzip
import shutil
import time
from subprocess import Popen, PIPE


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
