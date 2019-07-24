#!/usr/bin/python3
import configparser
import getopt
import os
import urllib
import sys
import time
from sync.projects import Projects
from sync.local import LocalFiles
from sync.listsmaker import ListsMaker
from sync.listsgetter import ListsGetter
from sync.sync import Sync


CONFIG_SECTIONS = {'dirs': ['mediadir', 'archivedir', 'listsdir'],
                   'urls': ['api_url', 'media_filelists_url', 'uploaded_media_url',
                            'foreignrepo_media_url'],
                   'limits': ['http_wait', 'http_retries', 'max_uploaded_gets',
                              'max_foreignrepo_gets'],
                   'misc': ['api_path', 'foreignrepo', 'agent']}


def usage(message=None):
    '''
    display a nice usage message along with an optional message
    describing an error
    '''
    if message:
        sys.stderr.write("%s\n" % message)
    usage_message = """Usage: $0 --configfile <path> [--projects] [--retries <num>]
          [--wait <num>] [--continue] [--verbose] [--dryrun]
or: $0 --help

This script retrieves information about media files uploaded or in use on a group of wikis,
compares that with the media files available on the local server, deletes any local files
that aren't used remotely, and downloads remote files that don't exist locally.

Arguments:
    --configfile (-c)    path to the configuration file with information about the
                         remote wikis, the local media directory tree, and so on
    --archive    (-a)    archive inactive projects; this is a slow operation
                         because detailed information about all active projects
                         must be retrieved via the MediaWiki api, one project
                         at a time
    --continue   (-C)    continue downloads from where the previous run, if any, left
                         off; this cannot be used with the 'full' option
    --full       (-f)    even if there is a previous run for the wikis to do, generate
                         all files from scratch as a full run; this cannot be used with
                         the 'continue' option
    --projects   (-p)    comma-separated list of projects to sync from, otherwise
                         all active remote projects will be synced from
    --retries    (-r)    the number of times to attempt to download a file before giving
                         up, in case of failure; if set here, this will override any
                         value in the config file
    --wait       (-w)    the number of seconds to wait between downloads; if set here,
                         this will override any value in the config file
    --verbose    (-v)    display various progress messages as the script runs
    --dryrun     (-d)    don't create or delete any files, show what would have been done
"""
    sys.stderr.write(usage_message)
    sys.exit(1)


def get_arg(opt, val, args):
    '''set one arg from opt/val'''
    if opt in ["-c", "--configfile"]:
        args['configfile'] = val
    elif opt in ["-p", "--projects"]:
        args['projects_todo'] = val.split(',')
    elif opt in ["-r", "--retries"]:
        args['retries'] = val
    elif opt in ["-w", "--wait"]:
        args['wait'] = val
    else:
        return False
    return True


def get_flag(opt, args):
    '''set one flag from opt'''
    if opt in ["-a", "--archive"]:
        args['archive'] = True
    elif opt in ["-C", "--continue"]:
        args['continue'] = True
    elif opt in ["-f", "--full"]:
        args['full'] = True
    elif opt in ["-v", "--verbose"]:
        args['verbose'] = True
    elif opt in ["-d", "--dryrun"]:
        args['dryrun'] = True
    elif opt in ["-h", "--help"]:
        usage('Help for this script\n')
    else:
        return False
    return True


def parse_args():
    '''get args passed on the command line
    and return as a dict'''
    args = {'verbose': False,
            'dryrun': False,
            'help': False,
            'archive': False,
            'continue': False,
            'full': False,
            'configfile': None,
            'projects_todo': None,
            'retries': None,
            'wait': None}
    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], "c:p:r:w:aCdvh", ["configfile=", "retries=", "wait=", "projects=",
                                            "archive", "continue", "full",
                                            "verbose", "dryrun", "help"])

    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))

    for (opt, val) in options:
        if not get_arg(opt, val, args) and not get_flag(opt, args):
            usage("Unknown option specified: <%s>" % opt)

    if remainder:
        usage("Unknown option(s) specified: {opt}".format(opt=remainder[0]))

    return args


def check_missing_args(args):
    '''check for mandatory args that are missing,
    and whine as needed'''
    if not args['configfile']:
        usage('The mandatory argument "--configfile" was not specified.')


def validate_args(args):
    '''validate arguments, whine about values
    as needed'''
    if args['retries']:
        if not args['retries'].isdigit():
            usage('--retries argument must be a positive integer')
        args['retries'] = int(args['retries'])
    if args['wait']:
        if not args['wait'].isdigit():
            usage('--wait argument must be a positive integer')
        args['wait'] = int(args['wait'])
    if args['continue'] and args['full']:
        usage('--full and --continue are mutually exclusive options')


def get_args():
    '''parse, validate, return command line args'''
    args = parse_args()
    check_missing_args(args)
    validate_args(args)
    return args


def get_config(configfile):
    '''read and parse config file entries'''
    parser = configparser.ConfigParser()
    parser.read(configfile)
    for section in CONFIG_SECTIONS:
        if not parser.has_section(section):
            sys.stderr.write("The mandatory configuration section "
                             + section + " was not defined.\n")
            raise configparser.NoSectionError(section)

    config = {}
    for section in CONFIG_SECTIONS:
        for setting in CONFIG_SECTIONS[section]:
            if parser.has_option(section, setting):
                config[setting] = parser.get(section, setting)
    return config


def validate_config(config):
    '''validate and convert config values'''
    for path in CONFIG_SECTIONS['dirs']:
        if not os.path.exists(config[path]):
            sys.stderr.write('No such path {path} for setting {setting}\n'.format(
                path=path, setting=config[path]))
            raise ValueError('Bad configfile setting')

    for url in CONFIG_SECTIONS['urls']:
        fields = urllib.parse.urlsplit(config[url])
        if not fields.scheme or not fields.netloc:
            sys.stderr.write('Invalid url {value} for setting {setting}\n'.format(
                value=config[url], setting=url))
            raise ValueError('Bad configfile setting')

    for number in CONFIG_SECTIONS['limits']:
        if not config[number].isdigit():
            sys.stderr.write('Setting {setting} must be a number, {value} given\n'.format(
                setting=number, value=config[number]))
            raise ValueError('Bad configfile setting')
        config[number] = int(config[number])

    for setting in CONFIG_SECTIONS['misc']:
        if not config[setting]:
            sys.stderr.write('Setting {setting} cannot be empty\n'.format(
                setting=config[setting]))
            raise ValueError('Bad configfile setting')


def merge_config(config, args):
    '''fold in any values from args that should be in the
    config settings, overriding settings from the configfile'''
    if args['retries']:
        config['http_retries'] = args['retries']
    if args['wait']:
        config['http_wait'] = args['retries']


def exclude_foreign_repo(config, active_projects):
    '''toss the foreign repo from the list of active
    projects. We won't mirror all that content!
    For Wikimedia project mirroring, the foreign repo
    would be commons.wikimedia.org (commonswiki).'''
    if config['foreignrepo']:
        if config['foreignrepo'] in active_projects:
            active_projects.pop(config['foreignrepo'], None)


def do_continue_downloads(args, config, projects, today):
    '''continue to retrieve media from remote, picking up
    from where last run left off, if any.'''
    syncer = Sync(config, projects, today, args['verbose'], args['dryrun'])

    if args['verbose']:
        print("continuing to download local media not on remote project")
    syncer.continue_getting_new_media()


def do_localmedia_prep(args, config, projects, today, most_recent_lists):
    '''do all the things to local media we want
    to do before sync (archive old crap, list out what
    we have that's good, etc)'''
    local = LocalFiles(config, projects, today, most_recent_lists, args['full'],
                       args['verbose'], args['dryrun'])
    if args['verbose']:
        print("setting up local media subdirectories")
    local.init_mediadirs()

    if args['archive']:
        if args['verbose']:
            print("archiving inactive projects")
        local.archive_inactive_projects()

    if args['verbose']:
        print("getting lists of local media")
    local.get_local_media_lists()
    if args['verbose']:
        print("sorting lists of local media")
    local.sort_local_media_lists()


def do_lists_retrieval(args, config, projects, today):
    '''get all the lists of media we need from the
    remote host'''
    getter = ListsGetter(config, projects, today, args['verbose'], args['dryrun'])

    if args['verbose']:
        print("getting lists of media uploaded to projects")
    getter.get_project_uploaded_media()
    if args['verbose']:
        print("getting lists of media uploaded to foreign repo")
    getter.get_project_foreignrepo_media()


def do_lists_generation(args, config, projects, today, most_recent_lists):
    '''generate all the lists of media we need: media to delete,
    media to retrieve, etc'''
    maker = ListsMaker(config, projects, today, args['full'], args['verbose'], args['dryrun'])

    if args['verbose']:
        print("cleaning up lists of media uploaded to projects")
    maker.cleanup_project_uploaded_media_lists()
    if args['verbose']:
        print("cleaning up lists of media uploaded to foreign repo")
    maker.cleanup_project_foreignrepo_media_lists()

    if args['verbose']:
        print("generating list of project-uploaded media to get")
        maker.generate_uploaded_files_to_get()
    if args['verbose']:
        print("generating list of foreign repo-uploaded media to get")
    maker.generate_foreignrepo_files_to_get()

    if args['verbose']:
        print("creating list of media to keep locally")
    maker.merge_media_files_to_keep()

    if args['verbose']:
        print("generating list of local media not on remote project")
    maker.list_local_media_not_on_remote()


def do_incr_lists_generation(args, config, projects, today, most_recent_lists):
    '''generate all the lists of media we need: media to delete,
    media to retrieve, etc'''
    maker = ListsMaker(config, projects, today, most_recent_lists,
                       args['full'], args['verbose'], args['dryrun'])

    if args['verbose']:
        print("cleaning up lists of media uploaded to projects")
    maker.cleanup_project_uploaded_media_lists()
    if args['verbose']:
        print("cleaning up lists of media uploaded to foreign repo")
    maker.cleanup_project_foreignrepo_media_lists()

    if args['verbose']:
        print("producing full list of media to keep")
    maker.merge_media_files_to_keep()

    if args['verbose']:
        print("updating lists of local media")
    maker.update_local_media_lists(most_recent_lists)
    if args['verbose']:
        print("producing full lists of local media not on remote project")
    # FIXME write this
    # maker.update_local_media_lists_not_on_remote()
    # ALSO need after this to do retrieves and deletes based on the new files...

    # CHECK ON ALL THIS STUFF, DO I NEED IT etc
    if args['verbose']:
        print("generating list of remote media gone since last run")
    maker.list_media_gone_from_remote(most_recent_lists)

    if args['verbose']:
        print("generating list of new project-uploaded media since last run")
    maker.list_new_uploaded_media_on_remote(most_recent_lists)
    if args['verbose']:
        print("generating list of new foreign-repo media since last run")
    maker.list_new_foreign_media_on_remote(most_recent_lists)


def do_sync(args, config, projects, today, most_recent_lists):
    '''retrieve media from remote, delete crap we don't need'''
    syncer = Sync(config, projects, today, most_recent_lists,
                  args['full'], args['verbose'], args['dryrun'])

    if args['verbose']:
        print("deleting local media not on remote project")
    syncer.delete_local_media_not_on_remote()

    if args['verbose']:
        print("downloading new media from remote")
    syncer.get_new_media()


def do_main():
    '''entry point'''
    args = get_args()
    config = get_config(args['configfile'])
    validate_config(config)
    merge_config(config, args)

    projects = Projects(config, args['projects_todo'], args['dryrun'])
    if args['verbose']:
        print("active projects are:", ",".join(projects.active.keys()))

    today = time.strftime("%Y%m%d", time.gmtime())
    most_recent_lists = ListsMaker.get_most_recent(config['listsdir'], today)
    if args['verbose']:
        print(most_recent_lists)

    if args['continue']:
        do_continue_downloads(args, config, projects, today)
    elif args['full']:
        do_localmedia_prep(args, config, projects, today, most_recent_lists)
        do_lists_retrieval(args, config, projects, today)
        do_lists_generation(args, config, projects, today, most_recent_lists)
        do_sync(args, config, projects, today, most_recent_lists)
    else:
        # FIXME if there are no full runs for a given wiki we should not proceed
        # but should do the full for that wiki

        # hoping for new lists here
        do_lists_retrieval(args, config, projects, today)
        # if there are not new lists we should not proceed, FIXME
        do_incr_lists_generation(args, config, projects, today, most_recent_lists)
        # the above rebuilds the full delete/keep lists with today's date so
        # they can be used for sync in the normal way
        do_sync(args, config, projects, today, most_recent_lists)


if __name__ == '__main__':
    do_main()
