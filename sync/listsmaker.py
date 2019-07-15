#!/usr/bin/python3
import os
import gzip
import glob
import sys
from subprocess import Popen, PIPE


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

    @staticmethod
    def get_most_recent(basedir, today, do_today=False):
        '''
        for all lists,
        find the most recent date each file was produced for each project,
        excluding today's run,
        creating a dict of projects vs dates, and returning the dict,
        or None if no list files have never been written
        '''
        list_date_info = {}
        date = None
        dates = os.listdir(basedir)
        dates = [date for date in dates if len(date) == 8 and date.isdigit()]
        for date in dates:
            if not do_today and date == today:
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

    @staticmethod
    def show_extra_entries(old_in, today_in, output):
        '''
        given two input filehandles to sorted entries, write to the
        output filehandle all entries in the first input file and not
        in the second
        '''
        newline = None
        while True:
            oldline = old_in.read()
            if not oldline:
                return
            while newline is None or newline < oldline:
                newline = today_in.read()
            if newline is None or newline > oldline:
                output.write(newline)

    def __init__(self, config, projects, today, most_recent_lists,
                 full=False, verbose=False, dryrun=False):
        '''
        configparser instance,
        Projects instance,
        foreign repo info,
        list of projects to actually operate on
        '''
        self.config = config
        self.projects = projects
        self.today = today
        self.full = full
        self.most_recent_lists = most_recent_lists
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
        todos = self.projects.get_todos()
        for project in todos:
            filename_base = in_filename_template.format(project=project)
            media_lists_path = os.path.join(self.config['listsdir'],
                                            self.today, project, filename_base)
            todays_files = glob.glob(media_lists_path)
            if not todays_files:
                print("warning: no media list files found of format", in_filename_template,
                      "for project", project)
                continue

            most_recent = sorted(todays_files)[-1]
            if not most_recent.endswith('.gz'):
                print("warning: bad filename found,", most_recent)
                continue
            newname = os.path.join(self.config['listsdir'], self.today, project,
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
        todos = self.projects.get_todos()
        for project in todos:
            if self.full or not ListsMaker.get_most_recent_file(
                    project, '-all-media-keep.gz', self.most_recent_lists):
                local_files_list = os.path.join(basedir, self.today, project,
                                                project + '_local_media_sorted.gz')
                uploaded_files_list = os.path.join(basedir, self.today, project,
                                                   project + '-uploads-sorted.gz')
                output_path = os.path.join(basedir, self.today, project,
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
        todos = self.projects.get_todos()
        for project in todos:
            if self.full or not ListsMaker.get_most_recent_file(
                    project, '-all-media-keep.gz', self.most_recent_lists):
                local_files_list = os.path.join(basedir, self.today, project,
                                                project + '_local_media_sorted.gz')
                foreignrepo_files_list = os.path.join(basedir, self.today, project,
                                                      project + '-foreignrepo-sorted.gz')
                output_path = os.path.join(basedir, self.today, project,
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
        todos = self.projects.get_todos()
        for project in todos:
            if self.full or not ListsMaker.get_most_recent_file(
                    project, '-all-media-keep.gz', self.most_recent_lists):
                basedir = os.path.join(self.config['listsdir'], self.today, project)
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
        todos = self.projects.get_todos()
        for project in todos:
            if self.full or not ListsMaker.get_most_recent_file(
                    project, '-all-media-keep.gz', self.most_recent_lists):
                basedir = os.path.join(self.config['listsdir'], self.today, project)
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

    def diff_lists(self, project, date, in_suffix, out_suffix, difftype='oldextra'):
        '''
        for a given project and date, find the list of media
        (<project> + in_suffix) for that project, generated on that date,
        and write out:
        for difftype 'oldextra', all entries in that file not in today's list
        for difftype 'newextra', all entries in today's list not in the old file
        '''
        oldfile = os.path.join(self.config['listsdir'], date, project,
                               project + in_suffix)
        todayfile = os.path.join(self.config['listsdir'], self.today, project,
                                 project + in_suffix)
        outfile = os.path.join(self.config['listsdir'], self.today, project,
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
                    if difftype == 'oldextra':
                        self.show_extra_entries(old_in, today_in, output)
                    elif difftype == 'newextra':
                        self.show_extra_entries(today_in, old_in, output)
                    else:
                        sys.stderr.write("unknown diff type {dtype} for {project}, {date}\n".format(
                            dtype=difftype, project=project, date=date))
                        sys.stderr.write("files: {in1}, {in2}, {out}\n".format(
                            in1=oldfile, in2=todayfile, out=outfile))
                        return

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
        todos = self.projects.get_todos()
        for project in todos:
            date = self.get_most_recent_file(project, '-all-media-keep.gz',
                                             most_recent_lists)
            if  date:
                self.diff_lists(project, date,
                                '-all-media-keep.gz', '-all-media-gone.gz',
                                'oldextra')

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
        todos = self.projects.get_todos()
        for project in todos:
            date = self.get_most_recent_file(project, '-uploads-sorted.gz',
                                             most_recent_lists)
            if  date:
                self.diff_lists(project, date,
                                '-uploads-sorted.gz', '-new-media-projectuploads.gz',
                                'newextra')

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
        todos = self.projects.get_todos()
        for project in todos:
            date = self.get_most_recent_file(project, '-foreignrepo-sorted.gz',
                                             most_recent_lists)
            if  date:
                self.diff_lists(project, date,
                                '-foreignrepo-sorted.gz', '-new-media-foreignrepouploads.gz',
                                'newextra')
