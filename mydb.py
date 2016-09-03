import sqlite3


class MyDB:
    """ DB manager"""

    # TODO: encapsulate data into a class
    repo_attributes = dict(commits='INT',
                           contributors='INT',
                           size='INT',
                           stars='INT',
                           watchers='INT',
                           forks='INT',
                           isfork='INT', )

    def __init__(self, filename='data.db'):
        self.file = filename

    def __enter__(self):
        self.conn = sqlite3.connect(self.file)
        self.c = self.conn.cursor()

        # http://stackoverflow.com/questions/3300464/how-can-i-get-dict-from-sqlite-query/3300514#3300514
        # https://docs.python.org/3/library/sqlite3.html#sqlite3.Row
        self.c_repo = self.conn.cursor()
        self.c_repo.row_factory = sqlite3.Row
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

    def createdb(self):
        self.c.execute('CREATE TABLE IF NOT EXISTS files (id INTEGER PRIMARY KEY, '
                       'name TEXT, repo_id INT, lang TEXT, url TEXT, hash TEXT, loc INT, location TEXT);')
        # TODO: construct stmt using repo_attributes
        self.c.execute('CREATE TABLE IF NOT EXISTS repos (url TEXT PRIMARY KEY, name TEXT, commits INT, '
                       'contributors INT, size INT, stars INT, watchers INT, forks INT, isfork INT);')
        self.c.execute('CREATE TABLE IF NOT EXISTS posts (id INTEGER PRIMARY KEY, type INT);')
        self.c.execute('CREATE TABLE IF NOT EXISTS file_post_xref (fid INTEGER, pid INTEGER, PRIMARY KEY(fid, pid));')
        self.conn.commit()

    def insertfile(self, fid=0, name="", repo="", repo_name="", lang="", url="",
                   hash="", loc=0, location="", posts=None):
        self.c.execute('INSERT OR IGNORE INTO repos (url, name) VALUES (?,?)',
                       (repo, repo_name))
        self.c.execute('SELECT oid FROM repos WHERE url=?', (repo,))
        repo_row = self.c.fetchone()
        if repo_row:
            repo_id = repo_row[0]
            self.c.execute('INSERT OR IGNORE INTO files VALUES (?,?,?,?,?,?,?,?)',
                           (fid, name, repo_id, lang, url, hash, loc, location,))
        else:
            print "Warning: Cannot find repo url: {0}".format(repo)

        if posts:
            for post_id, type_id in posts:
                self.c.execute('INSERT OR IGNORE INTO posts (id, type) VALUES (?,?)', (post_id, type_id))
                self.c.execute('INSERT OR IGNORE INTO file_post_xref (fid, pid) VALUES (?,?)', (fid, post_id,))

        self.conn.commit()

    def select_all_repos(self):
        props = ', '.join(self.repo_attributes.keys())
        stmt = 'SELECT oid as rid, url, {0} FROM repos ORDER BY oid;'.format(props)
        self.c_repo.execute(stmt)
        return self.c_repo.fetchall()

    def update_repo(self, rid, **kwargs):
        class SqlHelper:
            stmt = 'UPDATE repos SET {values} WHERE oid=?;'

            def __init__(self, rid):
                self.cols = []
                self.vals = [rid]

            def append_sql_arg(self, key, val):
                self.cols.insert(0, '{key}=?'.format(key=key))
                self.vals.insert(0, val)

            def get_stmt(self):
                return self.stmt.format(values=', '.join(self.cols))

            def get_args(self):
                return self.vals

        # TODO: do nothing if there's no update in the data
        helper = SqlHelper(rid)
        for kw in kwargs.iteritems():
            if kw[0] in self.repo_attributes:
                val = kw[1]
                if val is None:
                    continue
                if val is bool:
                    val = int(val)
                helper.append_sql_arg(kw[0], val)
            elif kw[0] in ('url', 'rid'):
                pass
            else:
                raise KeyError("key not expected: {0}".format(kw[0]))

        # print(helper.get_stmt(), helper.get_args())
        self.c.execute(helper.get_stmt(), helper.get_args())
        self.conn.commit()


def _test_create(fn):
    with MyDB(fn) as db:
        db.createdb()


def _test_insert_file(fn):
    with MyDB(fn) as db:
        db.insertfile(123, 'file-a', 'test-repo')
        db.insertfile(556, 'file-b', 'test-repo')


def _test_update_repo(fn):
    with MyDB(fn) as db:
        for row in db.select_all_repos():
            info = dict(row)
            info['commits'] = 5
            info['stars'] = 11
            db.update_repo(**info)
            # db.update_repo(rid, commits=2, stars=10, fff=0, isfork=False)


if __name__ == '__main__':
    fn = 'test.db'
    _test_create(fn)
    _test_insert_file(fn)
    _test_update_repo(fn)
