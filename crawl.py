#!/bin/env python

import json
import urllib
import re

from mydb import MyDB

search_url = 'https://searchcode.com/api/codesearch_I/?q={0}&p={1}&per_page={2}{3}'
test_file = 'file:///D:/MyProjects/Python/search-code-crawler/search-result.json'

code_url = 'https://searchcode.com/api/result/{0}/'
query = "stackoverflow.com"
# languages needed
# langs = [1, 2, 3]
langs = None
max_page = 2
per_page = 10
MAX_PAGE = 50
MAX_PER_PAGE = 100

max_page = min(max_page, MAX_PAGE)
per_page = min(per_page, MAX_PER_PAGE)


def main():
    with MyDB() as db:
        db.createdb()
        for i in range(max_page):
            url = construct_url(query, p=i, per_page=per_page, langs=langs)
            # print url

            # fh = urllib.urlopen(url)
            fh = urllib.urlopen(test_file)
            data = json.load(fh, encoding='utf-8')

            for r in data['results']:
                repo = r['repo']
                repo_name = r['name']
                view_url = r['url']
                lines = r['lines']
                fid = r['id']
                name = r['filename']
                loc = r['linescount']
                lang = r['language']
                hash = r['md5hash']
                location = r['location']

                qids = get_question_ids_from(lines)

                if len(qids) > 0:
                    # code has some real SO links
                    # print "qids:", qids
                    try:
                        code = retrieve_code(fid)
                        print "code:", code
                        # TODO: wirte code to file
                    except IOError as e:
                        print "Cannot get code content for file id: {fid}; Error:{e}" \
                            .format(fid=fid, e=e)

                    # store file and repo info into db
                    db.insertfile(fid=fid, name=name, repo=repo, repo_name=repo_name,
                                  lang=lang, url=view_url, hash=hash, loc=loc, location=location, qids=qids)
                else:
                    # False Positive
                    print "{0} contains no real SO link".format(str(fid))


def construct_url(query, p=0, per_page=100, langs=None):
    lang_part = ""
    if langs:
        for l in langs:
            lang_part += "&lan=" + str(l)
    return search_url.format(query, p, per_page, lang_part)


def retrieve_code(fid):
    url = code_url.format(fid)
    data = json.load(urllib.urlopen(url))
    return data["code"]


def get_so_qids(code):
    # pat = re.compile('https?://stackoverflow\.com/questions/\d+')
    # pat = re.compile('https?://stackoverflow\.com/questions/\d+/?(\S+)?')
    pat = re.compile('(https?://stackoverflow\.com/questions/(\d+)/?(?:\S+)?)')
    matches = pat.findall(code)
    if matches:
        return map(lambda x: x[1], matches)
        # return matches.group(0)
    else:
        return []


def get_question_ids_from(lines):
    qids = []
    for code_line in lines.values():
        qid = get_so_qids(code_line)
        if qid:
            qids += qid
    return qids


if __name__ == '__main__':
    main()
