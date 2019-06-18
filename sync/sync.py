#!/usr/bin/python3


class GetWebFile():
    '''methods for retrieving a file over http(s) and
storing it some nice place, with retries'''
    def __init__(self, config):
        self.config = config

    def retrieve_file(self, url, localpath):
        '''retrieve a file and put it in the
        right place, with retries and wait
        between retries as needed, returning True
        on success and False on final failure'''
        return True

    def try_retrieval(self, url, localpath):
        '''try to retrieve a file once; on
        success, put it in the right place,
        return True; on failure, remove any
        partial file created, return False'''
        # TODO FIXME how big are these files?
        # are we doing chunked retrievals?
        # does the request module let me tell
        # it to write output directly to a dir
        # or do I have to manage that? what
        # about https/certs?
        return True


class Sync():
    '''methods for syncing media from a remote MediaWiki
    instance or instances, to a local server'''

    def __init__(self, config, active_projects):
        self.config = config
        self.active_projects = active_projects

    def init_local_mediadirs(self):
        '''if there is no local basedir for media, or if
        any active project dir underneath it does not exist,
        create it and the associated hashdirs'''
        return

    def archive_inactive_projects(self):
        '''if a local directory references a project that
        is not active, if the directory is empty (the hash dirs
        underneath it are all empty), remove it; otherwise
        tar up the media and move it to the directory
        archive/inactive/projectname-date-media.tar.gz
        return'''

    def get_local_media_lists(self):
        '''write a list of all media for the local project,
        with path: basename, project name, hashdir and ctime
        so each entry will look like:
        01_Me_and_My_Microphone.ogg YYYYMMDDHHMMSS images/wikipedia/en/a/a6/
        these files will live in filelists/date/projectname/projectname_local_media.gz'''
        return

    def sort_local_media_lists(self):
        '''read and sort the local media lists'''
        return

    def get_project_uploaded_media(self):
        '''get via http from remote server the latest list
        of filenames/timestamps of media that was uploaded locally
        to each project, for all active projects'''
        return

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
