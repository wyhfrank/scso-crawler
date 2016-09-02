import sqlite3


class MyDB:
    """ DB manager"""

    def __init__(self, filename='data.db'):
        self.file = filename

    def __enter__(self):
        self.conn = sqlite3.connect(self.file)
        self.c = self.conn.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

    def createdb(self):
        self.c.execute('CREATE TABLE IF NOT EXISTS files (id INTEGER PRIMARY KEY, '
                       'name TEXT, repo_id INT, lang TEXT, url TEXT, hash TEXT, loc INT, location TEXT);')
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

    def select_all_repos(self, unprocessed_only=True):
        stmt = 'SELECT oid, url FROM repos ORDER BY oid;'
        if unprocessed_only:
            stmt = 'SELECT oid, url FROM repos WHERE commits IS NULL ORDER BY oid;'
        self.c.execute(stmt)
        return self.c.fetchall()

    def update_repo(self, rid, commits=None, contributors=None, size=None,
                    stars=None, watchers=None, forks=None, isfork=None):
        if commits is None:
            return

        if commits == -1:
            self.c.execute('UPDATE repos SET commits =? WHERE oid=?;', (commits, rid))
            self.conn.commit()
            return

        isfork_int = 0
        if isfork:
            isfork_int = 1

        self.c.execute('UPDATE repos SET commits =?, contributors =?, size=?, '
                       'stars=?, watchers=?, forks=?, isfork=? WHERE oid=?;', (commits,
                       contributors, size, stars, watchers, forks, isfork_int, rid))
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
        for rid, url in db.select_all_repos():
            db.update_repo(rid, -1)


if __name__ == '__main__':
    fn = 'test.db'
    _test_create(fn)
    _test_insert_file(fn)
    _test_update_repo(fn)