#!/bin/env python

import json
import urllib
import re
import os
import argparse
from pygithub3 import Github, exceptions as GitHubErrors
import requests
from bs4 import BeautifulSoup

from mydb import MyDB

# TODO: read config from file
# TODO: extract methods to modules
from crawl import src_dir_name, db_dir_name, db_file_name, mk_dirs, get_json_from_url

# repo_api = 'https://api.github.com/repos/{user_project}'
# contrib_api = 'https://api.github.com/repos/{user_project}/contributors'
url_template = 'https://github.com/{user}/{repo}'
config_file = 'config.json'
config = json.load(open(config_file))


class GitHubCrawler(object):
    def __init__(self, rows, callback, login=None, password=None):
        self.login = login
        self.password = password
        self.callback = callback
        self.rows = rows

    def set_rows(self, rows):
        self.rows = rows

    def start(self, force=False):
        gh = Github()
        gh.repos.set_credentials(login=self.login, password=self.password)

        total_count = len(self.rows)
        count = 0
        for row in self.rows:
            raw_url = row['url']
            # print raw_url
            username, reponame = self.parse_user_repo(raw_url)
            count += 1
            if not (username and reponame):
                print "[{current}/{total}]: illegal url: {url}" \
                    .format(current=count, total=total_count, url=raw_url)
                continue

            print "[{current}/{total}]: repo:{user}/{repo}" \
                .format(current=count, total=total_count, user=username, repo=reponame)

            key1 = 'stars'
            try:
                old_stars = row[key1]
                # print old_stars
                if self.need_to_update_value(old_stars, force):
                    try:
                        repo_data = gh.repos.get(user=username, repo=reponame)
                        tmp = self.retrieve_repo_basic_info(repo_data)
                        row.update(tmp)
                    except GitHubErrors.NotFound as e:
                        print "Repo not found: [{u}/{r}]".format(u=username, r=reponame)
                        row[key1] = -1
                    except requests.exceptions.ConnectionError as e:
                        print "Cannot connect to repo: {user}/{repo}".format(user=username, repo=reponame)
            except (KeyError, IndexError) as e:
                print "Warning: Key '{key}' not in data: {data}".format(key=key1, data=row)

            key2 = 'commits'
            try:
                old_commits = row[key2]
                if self.need_to_update_value(old_commits, force):
                    repo_url = self.construct_repo_url(username, reponame)
                    try:
                        commits, contribs = self.parse_commits_contribs(repo_url)
                        tmp = self.retrieve_repo_commit_info(commits, contribs)
                        row.update(tmp)
                    except IOError as e:
                        print "Cannot connect to {url}.".format(url=repo_url)
            except (KeyError, IndexError) as e:
                print "Warning: Key {key} not in data: {data}".format(key=key2, data=row)
            self.callback(row)

    @staticmethod
    def need_to_update_value(val, force, failed_val=-1):
        if force:
            return True

        if val is None:
            # val has not been processed yet
            return True
        elif val == failed_val:
            # failed last time
            return False
        else:
            # has a valid value (may be out-dated, though)
            return False

    @staticmethod
    def parse_user_repo(url):
        if url.find('github.com') != -1:
            if url.endswith('.git'):
                url = url[:-len('.git')]
            pat = re.compile('github\.com/([^/]+)/([^/]+)')
            match = pat.search(url)
            if match:
                return match.group(1), match.group(2)
        return None, None

    @staticmethod
    def construct_repo_url(user, repo):
        return url_template.format(user=user, repo=repo)

    @staticmethod
    def retrieve_repo_basic_info(repo_data):
        info = dict(
            stars=repo_data.stargazers_count,
            size=repo_data.size,
            watchers=repo_data.subscribers_count,
            forks=repo_data.forks_count,
            isfork=repo_data.fork,
        )
        return info

    @staticmethod
    def retrieve_repo_commit_info(commits, contributors):
        return dict(commits=commits, contributors=contributors)

    # The method to calculate the number of commits and contributors is not accurate using pygtihub3,
    # thus parse the repository page directly
    @staticmethod
    def parse_commits_contribs(repo_url):
        # with open(r'test-data\repo.html') as f:
        #     html = f.read()
        fh = urllib.urlopen(repo_url)
        html = fh.read()
        parsed_html = BeautifulSoup(html, 'html.parser')
        ul = parsed_html.body.find('ul', attrs={'class': 'numbers-summary'})
        commits = contribs = -1
        for i in ul.find_all('a'):
            num, name = list(i.stripped_strings)
            if name.find('commit') != -1:
                commits = int(num.replace(',', ''))
            elif name.find('contributor') != -1:
                contribs = int(num.replace(',', ''))

        return commits, contribs


def main():
    args = parse_arg()

    outdir, src_path, db_path = mk_dirs(args.outdir)
    with MyDB(os.path.join(db_path, db_file_name)) as db:
        rows = db.select_all_repos()

        gc = GitHubCrawler(rows, lambda r: db.update_repo(**r), config['login'], config['password'])
        gc.start(args.force)


def parse_arg():
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--outdir', dest='outdir', default='data',
                        help='specify the output directory (default: data)')
    parser.add_argument('-f', '--force-update', dest='force', default=False,
                        help='force updating data', action='store_true')
    args = parser.parse_args()
    return args


def _test():
    def process_data(row):
        print row

    rows = [
        {
            'url': 'http://github.com/wyhfrank/ninka',
            'rid': 1,
            'stars': None,
            'commits': None,
        }
    ]

    gc = GitHubCrawler(rows, process_data, config['login'], config['password'])
    gc.start()


if __name__ == '__main__':
    main()
    print "Done."
