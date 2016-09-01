#!/bin/env python

import json
import urllib
import re
import os
import argparse

from mydb import MyDB

search_url = 'https://searchcode.com/api/codesearch_I/?q={q}&p={p}&per_page={pp}{langs}'
# search_url = 'file:///D:/MyProjects/Python/search-code-crawler/search-result.json'

# TODO: move to config files
# TODO: What is the best practice to put global config variable
src_dir_name = 'src-files'
db_dir_name = 'db'
db_file_name = 'data.db'
# so_code_dir = 'snippets'

code_url = 'https://searchcode.com/api/result/{0}/'
query = "http stackoverflow com"

PAGE_RANGE_MIN = 0
PAGE_RANGE_MAX = 49
MAX_ITEM_PER_PAGE = 100
per_page = 100


def main():
    args = parse_arg()

    outdir, src_path, db_path = mk_dirs(args.outdir)
    with MyDB(os.path.join(db_path, db_file_name)) as db:
        db.createdb()
        for lang in args.langs:
            for i in range(args.p_start, args.p_end + 1):
                print "Processing [{now}/{all}].".format(now=i, all=args.p_end)

                url = construct_url(query, p=i, per_page=per_page, langs=[lang])
                print url
                data = get_json_from_url(url)

                if not data:
                    continue

                useful_count = 0
                for r in data['results']:
                    posts = get_posts_from(r['lines'])

                    # store file and repo info into db
                    para = extract_para(r, posts)
                    db.insertfile(**para)

                    if len(posts) > 0:
                        # code has some real SO links
                        write_code_to_file(para['fid'], src_path)
                        useful_count += 1
                print "{useful}/{total} are useful files on this page." \
                    .format(useful=useful_count, total=len(data['results']))


def get_json_from_url(url):
    try:
        fh = urllib.urlopen(url)
        text = fh.read()
        try:
            data = json.loads(text, encoding='utf-8')
            return data
        except ValueError as e:
            print "Cannot decode json for: {url}\n{text}".format(url=url, text=text)
    except IOError as e:
        print "Cannot connect to: {url}".format(url=url)


def extract_para(r, posts=None):
    para = dict()
    para['repo'] = r['repo']
    para['repo_name'] = r['name']
    para['url'] = r['url']
    para['fid'] = r['id']
    para['name'] = r['filename']
    para['loc'] = r['linescount']
    para['lang'] = r['language']
    para['hash'] = r['md5hash']
    para['location'] = r['location']
    para['posts'] = posts
    return para


def write_code_to_file(fid, src_path):
    fn = os.path.join(src_path, str(fid))
    if os.path.exists(fn):
        return
    try:
        url = code_url.format(fid)
        data = get_json_from_url(url)
        if not data:
            return

        code = data["code"]
        # print "code:", code
        try:
            with open(fn, 'w') as f:
                f.write(code.encode('utf-8'))
        except IOError as e:
            print "Cannot write to file: {fid}; Error:{e}" \
                .format(fid=fid, e=e)
    except IOError as e:
        print "Cannot get code content for file id: {fid}; Error:{e}" \
            .format(fid=fid, e=e)


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
    parser.add_argument('--langs', dest='langs', nargs='*', default = [22,19,23,6,21,24,16,15,32,28,51,144],
                        help='language filter, separate with space')
    args = parser.parse_args()
    args.p_start = max(PAGE_RANGE_MIN, min(args.p_start, PAGE_RANGE_MAX))
    args.p_end = max(PAGE_RANGE_MIN, min(args.p_end, PAGE_RANGE_MAX))
    args.langs = parse_langs(args.langs)
    return args


def parse_langs(langs_str):
    if not langs_str:
        return None
    # TODO: build the language dictionary
    lang_dict = dict(python=1)
    langs_int = []
    for lang in langs_str:
        val = None
        try:
            val = int(lang)
        except ValueError:
            try:
                val = lang_dict[lang]
            except KeyError as e:
                print "Language not supported:", lang
        if val is not None:
            langs_int.append(val)
    return langs_int


def construct_url(query, p=0, per_page=100, langs=None):
    lang_list = []
    if langs is not None:
        for lan in langs:
            lang_list.append(("lan", lan))
            # lang_part += "&lan=" + str(l)
    lang_part = "&" + urllib.urlencode(lang_list)
    # TODO: set to: Javascript, Python, Java, C#, Objective C, PHP, C++, C/C++ Header, Ruby, C, Perl, R
    # lang_part = "&lan=22&lan=19&lan=23&lan=6&lan=21&lan=24&lan=16&lan=15&lan=32&lan=28&lan=51&lan=144"
    query = urllib.quote_plus(query)
    return search_url.format(q=query, p=p, pp=per_page, langs=lang_part)


def get_so_post(code):
    # pat = re.compile('https?://stackoverflow\.com/questions/\d+')
    # pat = re.compile('https?://stackoverflow\.com/questions/\d+/?(\S+)?')
    q_pat = re.compile('https?://stackoverflow\.com/q(?:uestions)?/(\d+)')
    a_pat = re.compile('https?://stackoverflow\.com/a/(\d+)')
    to_find = [(q_pat, 1),  # question type id is 1
               (a_pat, 2)]  # answer type id is 2
    posts = []
    for t in to_find:
        matches = t[0].findall(code)
        posts += build_post_tuple(matches, t[1])
    return posts


def build_post_tuple(matches, type_id):
    posts = []
    if matches:
        for m in matches:
            posts.append((m, type_id))
    return posts


def get_posts_from(lines):
    posts = []
    for code_line in lines.values():
        post = get_so_post(code_line)
        if post:
            posts += post
    return posts


if __name__ == '__main__':
    main()
    print "Done."
