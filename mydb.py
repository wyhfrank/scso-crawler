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
        self.c.execute('CREATE TABLE IF NOT EXISTS questions (id INTEGER PRIMARY KEY);')
        self.c.execute('CREATE TABLE IF NOT EXISTS fq_xref (fid INTEGER, qid INTEGER, PRIMARY KEY(fid, qid));')
        self.conn.commit()

    def insertfile(self, fid=0, name="", repo="", repo_name="", lang="", url="",
                   hash="", loc=0, location="", qids=None):
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

        if qids:
            for qid in qids:
                self.c.execute('INSERT OR IGNORE INTO questions (id) VALUES (?)', (qid,))
                self.c.execute('INSERT OR IGNORE INTO fq_xref (fid, qid) VALUES (?,?)', (fid, qid,))

        self.conn.commit()


def db_test():
    with MyDB(r"test.db") as db:
        db.createdb()
        db.insertfile(123, 'file-a', 'test-repo')
        db.insertfile(556, 'file-b', 'test-repo')

if __name__ == '__main__':
    db_test()
