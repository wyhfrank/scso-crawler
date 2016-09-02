#!/bin/env python

import json
import urllib
import re
import os
import argparse
from pygithub3 import Github, exceptions as GitHubErrors

from mydb import MyDB

# TODO: read config from file
# TODO: extract methods to modules
from crawl import src_dir_name, db_dir_name, db_file_name, mk_dirs, get_json_from_url

# repo_api = 'https://api.github.com/repos/{user_project}'
# contrib_api = 'https://api.github.com/repos/{user_project}/contributors'
config_file = 'config.json'
config = json.load(open(config_file))

def main():
    args = parse_arg()

    outdir, src_path, db_path = mk_dirs(args.outdir)
    with MyDB(os.path.join(db_path, db_file_name)) as db:
        gh = Github()
        gh.repos.set_credentials(login=config['login'], password=config['password'])

        rows = db.select_all_repos()
        total_count = len(rows)
        count = 0
        for rid, repo_url in rows:
            count += 1
            print "[{current}/{total}]: id:{rid}, url:{url}"\
                .format(current=count, total=total_count, rid=rid, url=repo_url)
            username, reponame = retrieve_user_project(repo_url)
            if username is None:
                continue

            try:
                info=dict()
                repo_data = gh.repos.get(user=username, repo=reponame)
                tmp = retrieve_repo_basic_info(repo_data)
                info.update(tmp)

                contrib_list = gh.repos.list_contributors(user=username, repo=reponame).all()
                tmp = retrieve_repo_commit_info(contrib_list)
                info.update(tmp)

                info['rid'] = rid
                db.update_repo(**info)
            except GitHubErrors.NotFound as e:
                print "Repo not found: [{u}/{r}]".format(u=username, r=reponame)
                db.update_repo(rid, commits=-1)
            # break

def retrieve_repo_basic_info(repo_data):
    info = dict(
        stars=repo_data.stargazers_count,
        size=repo_data.size,
        watchers=repo_data.subscribers_count,
        forks=repo_data.forks_count,
        isfork=repo_data.fork,
    )
    return info

def retrieve_repo_commit_info(contrib_list):
    commits = 0
    contributors = 0
    for user in contrib_list:
        commits += user.contributions
        contributors += 1
    info = dict(commits=commits, contributors=contributors)
    return info

def retrieve_user_project(url):
    if url.find('github.com') != -1:
        pat = re.compile('github\.com/([^/]+)/(.+)\.git')
        match = pat.search(url)
        if match:
            return match.group(1), match.group(2)
    return None, None


def parse_arg():
    parser = argparse.ArgumentParser()
    parser.add_argument('--outdir', dest='outdir', default='data',
                        help='specify the output directory (default: data)')
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    main()
    print "Done."
