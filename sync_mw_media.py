#!/usr/bin/python3
import sys
from sync.sync import Sync


def usage(message=None):
    '''
    display a nice usage message along with an optional message
    describing an error
    '''
    if message:
        sys.stderr.write("%s\n" % message)
    sys.exit(1)


def parse_args():
    '''get args passed on the command line
    and return as a dict'''
    args = {'verbose': False,
            'help': False,
            'configfile': None,
            'retries': None,
            'wait': None}
    return args


def check_missing_conflicting_args(args):
    '''check for mandatory args that are
    missing, or args that conflict, and whine
    as needed'''
    return


def validate_args(args):
    '''validate arguments, whine about values
    as needed'''
    return


def get_args():
    '''parse, validate, return command line args'''
    args = parse_args()
    check_missing_conflicting_args(args)
    validate_args(args)
    return args


def get_config(configfile):
    '''read, parse and validate config file entries'''
    config = {}
    return config


def get_active_projects(config):
    '''get list of active projects from remote MediaWiki
    via the api, convert it to list of entries with format
    projecttype/langcode and return it'''
    active_projects = []
    return active_projects


def exclude_foreign_repo(config, active_projects):
    '''toss the foreign repo from the list of active
    projects. We won't mirror all that content!
    For Wikimedia project mirroring, the foreign repo
    would be commons.wikimedia.org (commonswiki).'''
    return []


def do_main():
    '''entry point'''
    args = get_args()
    config = get_config(args['config'])
    active_projects = get_active_projects(config)
    active_projects = exclude_foreign_repo(config, active_projects)
    syncer = Sync(config, active_projects)
    syncer.init_local_mediadirs()
    syncer.archive_inactive_projects()
    syncer.get_local_media_lists()
    syncer.sort_local_media_lists()
    syncer.get_project_uploaded_media()
    syncer.get_project_foreignrepo_media()
    syncer.cleanup_project_uploaded_media_lists()
    syncer.cleanup_project_foreignrepo_media_lists()
    syncer.generate_uploaded_files_to_get()
    syncer.generate_foreignrepo_files_to_get()
    syncer.merge_sort_files_to_keep()
    syncer.delete_local_media_not_on_remote()
    syncer.get_new_media()


if __name__ == '__main__':
    do_main()
