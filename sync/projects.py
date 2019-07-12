#!/usr/bin/python3
import json
import time
import requests
from sync.webgetter import WebGetter


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
    def exclude_foreign_repo(config, active_projects):
        '''toss the foreign repo from the list of active
        projects. We won't mirror all that content!
        For Wikimedia project mirroring, the foreign repo
        would be commons.wikimedia.org (commonswiki).'''
        if config['foreignrepo']:
            if config['foreignrepo'] in active_projects:
                active_projects.pop(config['foreignrepo'], None)

    @staticmethod
    def get_projecttype_from_url(url):
        '''give an url blah.wikisomething.org, dig the wikisomething
        piece out and return it. yes it stinks.'''
        # https://si.wikipedia.org
        return url.rsplit('.', 2)[1]

    def __init__(self, config, projects_todo, dryrun):
        '''
        args: config
              dict of active projects
              list of projects to do
        '''
        self.config = config
        self.dryrun = dryrun
        self.active = self.get_active_projects(projects_todo)
        self.exclude_foreign_repo(config, self.active)
        self.projecttypes_langcodes_to_dbnames = self.get_active_projects_by_projecttype_langcode()

    def get_projecttype_from_api(self, url, getter, session, project):
        '''given a url blah.wikisomething.org, get the filerepo info
        for it via the MediaWiki api, get the url entry from that,
        from that dig out the project type and return it. yes it stinks.'''
        # https://si.wikipedia.org

        filerepo_info_url = url + self.config['api_path']
        params = {'action': 'query', 'meta': 'filerepoinfo',
                  'friprop': 'name|url', 'format': 'json'}
        errors = 'Failed to retrieve file repo info for project: ' + project
        content = getter.get_content(filerepo_info_url, errors, session=session, params=params)
        try:
            repoinfo = json.loads(content)
            for repo in repoinfo['query']['repos']:
                if repo['name'] == 'local':
                    # "//upload.wikimedia.org/wikipedia/mediawiki"
                    # we assume that the project type is always the second to
                    # last field in the url. pretty gross.
                    fields = repo['url'].split('/')
                    return fields[-2]
        except Exception:
            # fixme should really display the issue here
            pass
        return None

    def get_siteinfo(self):
        '''get site info from MediaWiki via the api'''
        baseurl = (self.config['api_url'])
        params = {'action': 'sitematrix', 'format': 'json'}
        sess = requests.Session()
        sess.headers.update(
            {"User-Agent": self.config['agent'], "Accept": "application/json"})
        getter = WebGetter(self.config, dryrun=self.dryrun)
        errors = 'Failed to retrieve list of active projects'
        content = getter.get_content(baseurl, errors, session=sess, params=params)
        return json.loads(content)

    def process_special_sites(self, specials, active_projects, todo):
        '''turn the 'specials' section of the site matrix info into
        project info'''
        sess = requests.Session()
        sess.headers.update(
            {"User-Agent": self.config['agent'], "Accept": "application/json"})
        getter = WebGetter(self.config, dryrun=self.dryrun)

        for site in specials:
            if 'private' in site:
                continue
            active_projects[site['dbname']] = {'langcode': site['code']}

            if not todo or site['dbname'] in todo:
                active_projects[site['dbname']]['todo'] = True
            else:
                continue

            active_projects[site['dbname']]['projecttype'] = self.get_projecttype_from_api(
                site['url'], getter, sess, site['dbname'])
            # this is us being nice to the remote servers
            time.sleep(self.config['http_wait'])

    def process_regular_site(self, regular_site, active_projects, todo):
        '''turn section for one site from site matrix info into project info'''
        # there is a 'count' entry which doesn't have site info. might be others too.
        try:
            if 'site' in regular_site:
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
                for site in regular_site['site']:
                    active_projects[site['dbname']] = {
                        'projecttype': self.get_projecttype_from_url(site['url']),
                        'langcode': regular_site['code']}

                    if not todo or site['dbname'] in todo:
                        active_projects[site['dbname']]['todo'] = True
        except TypeError:
            return

    def get_active_projects(self, todo):
        '''get list of active projects from remote MediaWiki
        via the api. make a dict with key the dbname and containing langcode for each wiki
        and projecttype for those for which it is cheap (not 'specials') and for those which
        are in the todo list even if not cheap,
        return the dict'''

        siteinfo = self.get_siteinfo()

        active_projects = {}
        for sitegroup in siteinfo['sitematrix']:
            if sitegroup == 'specials':
                self.process_special_sites(siteinfo['sitematrix'][sitegroup], active_projects, todo)
            else:
                self.process_regular_site(siteinfo['sitematrix'][sitegroup], active_projects, todo)
        return active_projects

    def fill_in_projecttypes(self):
        '''
        only if you really need it. expensive.
        for every project in self.active, without a project type, get the project type
        via the MediaWiki api and stash in the project entry in self.active
        '''
        siteinfo = self.get_siteinfo()

        sess = requests.Session()
        sess.headers.update(
            {"User-Agent": self.config['agent'], "Accept": "application/json"})
        getter = WebGetter(self.config, dryrun=self.dryrun)

        for sitegroup in siteinfo['sitematrix']:
            if sitegroup == 'specials':
                for site in siteinfo['sitematrix'][sitegroup]:
                    if site['dbname'] not in self.active:
                        continue
                    self.active[site['dbname']]['projecttype'] = self.get_projecttype_from_api(
                        site['url'], getter, sess, site['dbname'])
                    # this is us being nice to the remote servers yet again
                    time.sleep(self.config['http_wait'])

        # great, we got that. now redo self.projecttypes_langcodes_to_dbnames
        self.projecttypes_langcodes_to_dbnames = self.get_active_projects_by_projecttype_langcode()

    def get_active_projects_by_projecttype_langcode(self):
        '''convert the active projects dict into one that uses the projecttype and langcode
        for a key and spits out the dbname (projectname) instead
        ome of these entries will not have a projecttype, just return empty for those'''
        to_return = {}
        for dbname in self.active:
            if 'projecttype' in self.active[dbname]:
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
        commonswiki -> wikipedia, commons
        If we were passed a dbname but the dbname is not in the dict of active
        projects, then request the information via the mediawiki api'''
        if '/' in project:
            projecttype, langcode = project.split('/')
        elif project in self.active:
            projecttype = self.active[project]['projecttype']
            langcode = self.active[project]['langcode']
        else:
            # FROMHERE
            pass
        return (projecttype, langcode)

    def get_todos(self):
        '''
        return only active projects that are marked 'todo', or
        if none are, return everything (it means the caller
        wanted them all)
        '''
        all_projects = self.active.keys()
        todo = [project for project in all_projects if 'todo' in self.active[project]]
        if not todo:
            todo = all_projects
        return todo
