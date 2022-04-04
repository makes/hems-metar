import os
import sys
import sqlite3

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python index_db.py <db_file>")
        exit()
    dbfile = sys.argv[1]
    if not os.path.exists(dbfile):
        print(f"File {dbfile} does not exist")
        exit()
    statements = ["CREATE INDEX icao_metar ON metar (icao)",
                  "CREATE INDEX icao_time_metar ON metar (icao, time)",
                  "CREATE INDEX icao_taf ON taf (icao)",
                  "CREATE INDEX icao_time_taf ON taf (icao, time)"]
    con = sqlite3.connect(dbfile)
    cur = con.cursor()
    for sql in statements:
        print(f'Executing "{sql}"')
        cur.execute(sql)
        con.commit()
    con.close()