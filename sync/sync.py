#!/usr/bin/python3
import os
import gzip
import hashlib
import shutil
import time
import urllib
from sync.webgetter import WebGetter
from sync.listsmaker import ListsMaker
from sync.local import LocalFiles


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

    @staticmethod
    def get_last_entry(path):
        '''read and return the last entry from a
        gzipped file. expect the format to be
        'something'<whitespace>something else
        Pretty awful isn't it'''
        last_entry = None
        with gzip.open(path, "rb") as infile:
            while True:
                entry = infile.readline()
                if not entry:
                    return last_entry
                last_entry = entry.rstrip().split()[0]
                if last_entry.startswith(b"'") and last_entry.endswith(b"'"):
                    last_entry = last_entry[1:-1]
        return None

    @staticmethod
    def find_entry_in_file(fhandle, to_find):
        '''find the last entry in a gzipped file
        and return it. the slow tedious way.'''
        while True:
            entry = fhandle.readline()
            if not entry:
                return False
            entry = entry.rstrip()
            if entry == to_find:
                return True

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
        todos = self.projects.get_todos()
        for project in todos:
            basedir = os.path.join(self.config['listsdir'], self.today, project)
            if self.full or not ListsMaker.get_most_recent_file(
                    project, '-all-media-keep.gz', self.most_recent_lists):
                deletes_list = os.path.join(basedir, project + '-all-media-delete.gz')
            else:
                # this is the incremental list
                deletes_list = os.path.join(basedir, project + '-all-media-gone.gz')
            (projecttype, langcode) = self.projects.get_projecttype_langcode(project)
            archived_deletes_dir = os.path.join(self.config['archivedir'], 'deleted',
                                                projecttype, langcode)
            if self.dryrun:
                print("would move entries in {deletes} to {archived} ".format(
                    deletes=deletes_list, archived=archived_deletes_dir))
                return
            if not os.path.exists(archived_deletes_dir):
                os.makedirs(archived_deletes_dir)
                LocalFiles.init_hashdirs(archived_deletes_dir, self.dryrun)
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
            # in case we have a file with filename<whitespace>timestamp<stuff> in it.
            toget = toget.split()[0]
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
                fhandles['fail_out'].write(("'%s' [%d] %s\n" % (
                    toget.decode('utf-8'), resp_code, url)).encode('utf-8'))

                fhandles['fail_out'].write("'{filename}' [{code}] {url}\n".format(
                    filename=toget, url=url, code=resp_code).encode('utf-8'))
                if resp_code == 404:
                    # don't count missing files against our get count, they are
                    # probably junk links
                    gets -= 1
            else:
                fhandles['retr_out'].write(("'%s' %s\n" % (
                    toget.decode('utf-8'), url)).encode('utf-8'))
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
        todos = self.projects.get_todos()
        for project in todos:
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
        todos = self.projects.get_todos()
        for project in todos:
            files['retrieved'] = os.path.join(basedir, self.today, project,
                                              project + '_local_retrieved.gz')
            files['failed'] = os.path.join(basedir, self.today, project,
                                           project + '_local_get_failed.gz')
            if self.full or not ListsMaker.get_most_recent_file(
                    project, '-all-media-keep.gz', self.most_recent_lists):
                files['toget'] = os.path.join(basedir, self.today, project,
                                              project + '-uploaded-toget.gz')
            else:
                # this is the incremental file
                files['toget'] = os.path.join(basedir, self.today, project,
                                              project + '-new-media-projectuploads.gz')
            self.get_new_media_from_list(max_local_gets, 'local', files)
            files['retrieved'] = os.path.join(basedir, self.today, project,
                                              project + '_foreignrepo_retrieved.gz')
            files['failed'] = os.path.join(basedir, self.today, project,
                                           project + '_foreignrepo_get_failed.gz')
            if self.full or not ListsMaker.get_most_recent_file(
                    project, '-all-media-keep.gz', self.most_recent_lists):
                files['toget'] = os.path.join(basedir, self.today, project,
                                              project + '-foreignrepo-toget.gz')
            else:
                # this is the incremental file
                files['toget'] = os.path.join(basedir, self.today, project,
                                              project + 'new-media-foreignrepouploads.gz')
            self.get_new_media_from_list(max_foreignrepo_gets, 'foreignrepo', files)

    def continue_getting_new_media(self):
        '''
        for each project todo, find the last file we successfully downloaded for
        project-uploaded media, check the list of files to download for that date,
        locate the entry in that file with the last file we downloaded, and continue
        downloads from there. Then repeat for foreign-repo-uploaded media (yet to be
        implemented). Log all downloads to success/failure logs for today.
        We only look at full lists to download, not incremental lists (this needs
        also to be implemented, but with care).
        '''
        basedir = self.config['listsdir']
        lists_by_date = ListsMaker.get_most_recent(self.config['listsdir'], today=True)
        good_projectuploaded_gets_file = '_local_retrieved.gz'
        good_foreignrepouploaded_gets_file = '_foreignrepo_retrieved.gz'

        fhandles = {}
        todos = self.projects.get_todos()
        for project in todos:
            # first find out when we last downloaded something successfully
            # from that site
            projectuploads_gets_date = ListsMaker.get_most_recent_file(
                project, good_projectuploaded_gets_file, lists_by_date)
            foreignrepouploads_gets_date = ListsMaker.get_most_recent_file(
                project, good_foreignrepouploaded_gets_file, lists_by_date)

            # project-uploaded files first
            to_get_list = os.path.join(basedir, projectuploads_gets_date, project,
                                       project + '-uploaded-toget.gz')
            retrieved_list = os.path.join(basedir, projectuploads_gets_date, project,
                                          project + '_local_retrieved.gz')
            failed_list = os.path.join(basedir, projectuploads_gets_date, project,
                                       project + '_local_get_failed.gz')

            downloaded_last = self.get_last_entry(os.path.join(
                basedir, projectuploads_gets_date,
                project, project + good_projectuploaded_gets_file))

            if self.verbose:
                print("checked", os.path.join(basedir, projectuploads_gets_date,
                                              project, project + good_projectuploaded_gets_file),
                      "for last downloaded:", downloaded_last)

            if downloaded_last is None:
                continue

            with gzip.open(to_get_list, "rb") as fhandles['toget_in']:
                if not self.find_entry_in_file(fhandles['toget_in'], downloaded_last):
                    continue
                if self.dryrun:
                    print("would download media after", downloaded_last, "from",
                          to_get_list, 'with logging to', retrieved_list,
                          "and", failed_list)
                    continue
                if self.verbose:
                    print("downloading media after", downloaded_last, "from",
                          to_get_list, ' with logging to', retrieved_list,
                          "and", failed_list)

                retr_mode = "wb"
                if os.path.exists(retrieved_list):
                    retr_mode = "ab"

                failed_mode = "wb"
                if os.path.exists(failed_list):
                    failed_mode = "ab"

                with gzip.open(retrieved_list, retr_mode) as fhandles['retr_out']:
                    with gzip.open(failed_list, failed_mode) as fhandles['fail_out']:
                        self.get_new_media_for_project(
                            'local', project, self.config['max_uploaded_gets'], fhandles)

            # foreignrepo-uploaded files next
            to_get_list = os.path.join(basedir, foreignrepouploads_gets_date, project,
                                       project + '-foreignrepo-toget.gz')
            retrieved_list = os.path.join(basedir, foreignrepouploads_gets_date, project,
                                          project + '_foreignrepo_retrieved.gz')
            failed_list = os.path.join(basedir, foreignrepouploads_gets_date, project,
                                       project + '_foreignrepo_get_failed.gz')

            downloaded_last = self.get_last_entry(os.path.join(
                basedir, foreignrepouploads_gets_date,
                project, project + good_foreignrepouploaded_gets_file))

            print("checked", os.path.join(basedir, foreignrepouploads_gets_date,
                                          project, project + good_foreignrepouploaded_gets_file),
                  "for last downloaded:", downloaded_last)

            if downloaded_last is None:
                continue

            with gzip.open(to_get_list, "rb") as fhandles['toget_in']:
                if not self.find_entry_in_file(fhandles['toget_in'], downloaded_last):
                    continue
                if self.dryrun:
                    print("would download media after", downloaded_last, "from",
                          to_get_list, 'with logging to', retrieved_list,
                          "and", failed_list)
                    continue
                if self.verbose:
                    print("downloading media after", downloaded_last, "from",
                          to_get_list, ' with logging to', retrieved_list,
                          "and", failed_list)

                retr_mode = "wb"
                if os.path.exists(retrieved_list):
                    retr_mode = "ab"

                failed_mode = "wb"
                if os.path.exists(failed_list):
                    failed_mode = "ab"

                with gzip.open(retrieved_list, retr_mode) as fhandles['retr_out']:
                    with gzip.open(failed_list, failed_mode) as fhandles['fail_out']:
                        self.get_new_media_for_project(
                            'local', project, self.config['max_uploaded_gets'], fhandles)
