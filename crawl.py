#!/bin/env python

import json
import urllib
import re
import os
import argparse

from mydb import MyDB

search_url = 'https://searchcode.com/api/codesearch_I/?q={q}&p={p}&per_page={pp}{langs}'
# search_url = 'file:///D:/MyProjects/Python/search-code-crawler/search-result.json'

src_dir_name = 'src-files'
db_dir_name = 'db'
db_file_name = 'data.db'
# so_code_dir = 'snippets'

code_url = 'https://searchcode.com/api/result/{0}/'
query = "stackoverflow.com"

PAGE_RANGE_MIN = 0
PAGE_RANGE_MAX = 49
MAX_ITEM_PER_PAGE = 100
per_page = 2

def main():
    args = parse_arg()

    outdir, src_path, db_path = mk_dirs(args.outdir)
    with MyDB(os.path.join(db_path, db_file_name)) as db:
        db.createdb()
        for i in range(args.p_end + 1):
            url = construct_url(query, p=i, per_page=per_page, langs=args.langs)
            # print url

            fh = urllib.urlopen(url)
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

                try:
                    code = retrieve_code(fid)
                    # print "code:", code
                    fn = os.path.join(src_path, str(fid))
                    with open(fn, 'w') as f:
                        f.write(code.encode('utf-8'))
                except IOError as e:
                    print "Cannot get code content for file id: {fid}; Error:{e}" \
                        .format(fid=fid, e=e)

                if len(qids) > 0:
                    # store file and repo info into db
                    # code has some real SO links
                    # print "qids:", qids
                    db.insertfile(fid=fid, name=name, repo=repo, repo_name=repo_name,
                                  lang=lang, url=view_url, hash=hash, loc=loc, location=location, qids=qids)
                else:
                    # False Positive
                    print "{0} contains no real SO link".format(str(fid))


def mk_dirs(outdir):
    dirs_to_make = [outdir,
                    os.path.join(outdir, src_dir_name),
                    os.path.join(outdir, db_dir_name),
                    ]
    for d in dirs_to_make:
        if not os.path.exists(d):
            os.mkdir(d)
    return dirs_to_make


def parse_arg():
    parser = argparse.ArgumentParser()
    parser.add_argument('--outdir', dest='outdir', default='data',
                        help='specify the output directory (default: data)')
    parser.add_argument('--page-start', dest='p_start', default=0, type=int,
                        help='start from page N (default: 0)', metavar='N')
    parser.add_argument('--page-end', dest='p_end', default=49, type=int,
                        help='end with page N (default: 49)', metavar='N')
    parser.add_argument('--langs', dest='langs', nargs='*',
                        help='language filter, separate with space')
    args = parser.parse_args()
    args.p_start = max(PAGE_RANGE_MIN, min(args.p_start, PAGE_RANGE_MAX))
    args.p_end = max(PAGE_RANGE_MIN, min(args.p_end, PAGE_RANGE_MAX))
    args.langs = parse_langs(args.langs)
    return args

def parse_langs(langs_str):
    if not langs_str:
        return None
    lang_dict = dict(python=1)
    langs_int = []
    for lang in langs_str:
        try:
            langs_int.append(lang_dict[lang])
        except :
            print "Language not supported:", lang
    return langs_int


def construct_url(query, p=0, per_page=100, langs=None):
    lang_part = ""
    if langs:
        for l in langs:
            lang_part += "&lan=" + str(l)
    return search_url.format(q=query, p=p, pp=per_page, langs=lang_part)


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
