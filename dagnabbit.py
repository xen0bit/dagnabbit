#! /usr/bin/env python3
import sys
import csv
import json
import sqlite3
import argparse


def dfs_json(json_obj, level=0, path="<root>", parent_key="<root>"):
    if isinstance(json_obj, dict):
        for key, value in json_obj.items():
            if not (isinstance(value, dict) or isinstance(value, list)):
                yield level, path, parent_key, key
                yield level + 1, path, key, f"{value}"
            else:
                yield level, path, parent_key, key
                yield from dfs_json(
                    value, level + 1, f"{path}.{key}" if path != "<root>" else key, key
                )
    elif isinstance(json_obj, list):
        for idx, value in enumerate(json_obj):
            if not (isinstance(value, dict) or isinstance(value, list)):
                yield level - 1, path, parent_key, f"{value}"
            else:
                yield from dfs_json(value, level, f"{path}[{idx}]", f"{parent_key}")


def read_json_lines(cur):
    cur.execute(
        """
                    CREATE TABLE IF NOT EXISTS "edges" (
                        "id"	INTEGER,
                        "level"	INTEGER,
                        "path"	TEXT,
                        "src"	TEXT,
                        "dst"	TEXT,
                        "ct"	INTEGER,
                        PRIMARY KEY("id" AUTOINCREMENT),
                        UNIQUE("level","src","dst")
                    );
                    """
    )
    for line in sys.stdin:
        try:
            # Strip newline characters and parse JSON
            json_data = json.loads(line.strip())
            for level, path, parent_key, child_key in dfs_json(json_data):
                # print(f"Level: {level}, Path: {path}, Parent Key: {parent_key}, Child Key: {child_key}")
                cur.execute(
                    """
                            INSERT INTO edges (level, path, src, dst, ct)
                            VALUES (?, ?, ?, ?, ?)
                            ON CONFLICT (level, src, dst) DO UPDATE SET
                                ct = ct + 1;
                """,
                    (level, path, parent_key, child_key, 1),
                )
            # Handle the parsed JSON data
            # print(json_data)
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON: {e}", file=sys.stderr)

    conn.commit()

def get_max_level(cur):
    res = cur.execute('''
        SELECT max(level) FROM edges;
    ''')
    return res.fetchone()[0]


def export_sqlite(conn):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT level || ':' || SUBSTR(src, 1, 150), (level+1) || ':' || SUBSTR(dst, 1, 150), ct FROM edges
    """
    )
    with sqlite3.connect("edges.db") as dstconn:
        dstcur = dstconn.cursor()
        dstcur.execute(
            """
                CREATE TABLE IF NOT EXISTS "edges" (
                    "src"	TEXT,
                    "dst"	TEXT,
                    "ct"	INTEGER
                );
                """
        )

        dstcur.executemany(
            """
                            INSERT INTO edges (src, dst, ct)
                            VALUES (?, ?, ?)
        """,
            cur,
        )
        dstconn.commit()
    dstconn.close()


def export_json(conn):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT level || ':' || SUBSTR(src, 1, 150), (level+1) || ':' || SUBSTR(dst, 1, 150), ct FROM edges
    """
    )
    #Memory efficient json writing
    with open("edges.json", "w") as f:
        f.write('{"edges":[')
        for row in cur:
            f.write(json.dumps({"src": row[0], "dst": row[1], "ct": row[2]}) + ',')
        f.seek(f.tell()-1)
        f.write(']}')

def export_csv(conn):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT level || ':' || SUBSTR(src, 1, 150), (level+1) || ':' || SUBSTR(dst, 1, 150), ct FROM edges
    """
    )
    header = ['src', 'dst', 'ct']
    with open("edges.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(cur)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("fmt", help="output format for edges [sqlite, json, csv]")
    args = parser.parse_args()
    with sqlite3.connect("staging.db") as conn:
        read_json_lines(conn.cursor())
        print(get_max_level(conn.cursor()))
        if args.fmt == "sqlite":
            export_sqlite(conn)
        elif args.fmt == "json":
            export_json(conn)
        elif args.fmt == "csv":
            export_csv(conn)
    conn.close()
