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
        self.c.execute('CREATE TABLE IF NOT EXISTS repos (url TEXT PRIMARY KEY, name TEXT, commits INT);')
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


def db_test():
    with MyDB(r"test.db") as db:
        db.createdb()
        db.insertfile(123, 'file-a', 'test-repo')
        db.insertfile(556, 'file-b', 'test-repo')

if __name__ == '__main__':
    db_test()
