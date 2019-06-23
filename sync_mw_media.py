#!/usr/bin/python3
import configparser
import getopt
import json
import os
import urllib
import sys
import time
import requests
from sync.sync import Sync


CONFIG_SECTIONS = {'dirs': ['mediadir', 'archivedir', 'listsdir'],
                   'urls': ['api_url', 'foreignrepo_media_url', 'uploaded_media_url'],
                   'limits': ['http_wait', 'http_retries', 'max_uploaded_gets',
                              'max_foreignrepo_gets'],
                   'misc': ['foreignrepo', 'agent']}


def usage(message=None):
    '''
    display a nice usage message along with an optional message
    describing an error
    '''
    if message:
        sys.stderr.write("%s\n" % message)
    usage_message = """Usage: $0 --configfile <path> [--projects] [--retries <num>] [--wait <num>]
          [--verbose]
or: $0 --help

This script retrieves information about media files uploaded or in use on a group of wikis,
compares that with the media files available on the local server, deletes any local files
that aren't used remotely, and downloads remote files that don't exist locally.

Arguments:
    --configfile (-c)    path to the configuration file with information about the
                         remote wikis, the local media directory tree, and so on
    --projects   (-p)    comma-separated list of projects to sync from, otherwise
                         all active remote projects will be synced from
    --retries    (-r)    the number of times to attempt to download a file before giving
                         up, in case of failure; if set here, this will override any
                         value in the config file
    --wait       (-w)    the number of seconds to wait between downloads; if set here,
                         this will override any value in the config file
    --verbose    (-v)    display various progress messages as the script runs
"""
    sys.stderr.write(usage_message)
    sys.exit(1)


def parse_args():
    '''get args passed on the command line
    and return as a dict'''
    args = {'verbose': False,
            'help': False,
            'configfile': None,
            'projects_todo': None,
            'retries': None,
            'wait': None}
    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], "c:p:r:w:vh", ["configfile=", "retries=", "wait=",
                                         "projects=", "verbose", "help"])

    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))

    for (opt, val) in options:
        if opt in ["-c", "--configfile"]:
            args['configfile'] = val
        elif opt in ["-p", "--projects"]:
            args['projects_todo'] = val.split(',')
        elif opt in ["-r", "--retries"]:
            args['retries'] = val
        elif opt in ["-w", "--wait"]:
            args['wait'] = val
        elif opt in ["-v", "--verbose"]:
            args['verbose'] = True
        elif opt in ["-h", "--help"]:
            usage('Help for this script\n')
        else:
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


def get_projecttype_from_url(url):
    '''give an url blah.wikisomething.org, dig the wikisomething
    piece out and return it. yes it stinks.'''
    return url.rsplit('.', 2)[0]


def get_active_projects(config):
    '''get list of active projects from remote MediaWiki
    via the api, convert it to a dict of entries with key the dbname
    and containing projecttype, langcode for each wiki, and return it'''

    baseurl = (config['api_url'])
    params = {'action': 'sitematrix', 'format': 'json'}
    sess = requests.Session()
    sess.headers.update(
        {"User-Agent": config['agent'], "Accept": "application/json"})

    retried = 0
    done = False
    while not done and retried < config['http_retries']:
        response = sess.get(baseurl, params=params, timeout=5)
        if response.status_code == 200:
            done = True
        else:
            retried += 1
            time.sleep(config['http_wait'])
    if not done:
        sys.stderr.write('Failed to retrieve list of active projects ' +
                         '(response code: {code}\n'.format(code=response.status_code))
        response.raise_for_status()

    siteinfo = json.loads(response.content)
    active_projects = {}
    for sitegroup in siteinfo['sitematrix']:
        if sitegroup == 'specials':
            for site in siteinfo['sitematrix'][sitegroup]:
                active_projects[site['dbname']] = {
                    'projecttype': get_projecttype_from_url(site['url']),
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
                        'projecttype': get_projecttype_from_url(site['url']),
                        'langcode': siteinfo['sitematrix'][sitegroup]['code']}
        except TypeError:
            continue
    return active_projects


def exclude_foreign_repo(config, active_projects):
    '''toss the foreign repo from the list of active
    projects. We won't mirror all that content!
    For Wikimedia project mirroring, the foreign repo
    would be commons.wikimedia.org (commonswiki).'''
    if config['foreignrepo']:
        if config['foreignrepo'] in active_projects:
            active_projects.pop(config['foreignrepo'], None)


def do_main():
    '''entry point'''
    args = get_args()
    config = get_config(args['configfile'])
    validate_config(config)
    merge_config(config, args)

    active_projects = get_active_projects(config)
    exclude_foreign_repo(config, active_projects)
    if args['verbose']:
        print("active projects are:", ",".join(active_projects.keys()))

    syncer = Sync(config, active_projects, args['projects_todo'])
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
