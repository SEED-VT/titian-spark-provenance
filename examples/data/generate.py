#!/usr/bin/env python3
"""Generate the BigSift CLI demo datasets at scale (one injected fault each).

    python3 examples/data/generate.py [rows]      # default 1,000,000

Writes airport.csv, weather.csv, student.txt next to this script. The files are
gitignored (too large to commit); bin/bigsift runs this automatically if they are
missing.
"""
import os
import random
import sys

ROWS = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000
HERE = os.path.dirname(os.path.abspath(__file__))
random.seed(7)

airports = ["LAX", "SEA", "JFK", "ORD", "ATL", "MNN"]
majors = ["CS", "EE", "ME", "IE"]


def airport(f):
    for i in range(ROWS - 1):
        ap = random.choice(airports); h = random.randint(6, 21)
        am = random.randint(0, 30); dur = random.randint(5, 40)
        dm = am + dur; dh = h + dm // 60; dm %= 60
        f.write(f"3/{i % 28 + 1}/17,{100000 + i},{h}:{am:02d},{dh}:{dm:02d},{ap}\n")
    # fault: a transit crossing midnight, parsed as a large negative layover
    f.write("11/9/12,141011,23:53,1:23,MNN\n")


def weather(f):
    for _ in range(ROWS - 1):
        zip = random.choice(["90210", "10001", "60601", "33101"])
        d = random.randint(1, 28)
        snow = f"{random.randint(0, 160)}mm" if random.random() < 0.5 \
            else f"{round(random.uniform(0, 0.5), 2)}ft"
        f.write(f"{zip},{d}/12/2015,{snow}\n")
    # fault: 90 inches, which convert_to_mm misreads as feet (x304.8 -> 27432mm)
    f.write("90210,25/12/2015,90in\n")


def student(f):
    for i in range(ROWS - 1):
        g = random.choice(["male", "female"]); age = random.randint(18, 24)
        grade = random.randint(0, 3)
        f.write(f"s{i} n{i} {g} {age} {grade} {random.choice(majors)}\n")
    # fault: an invalid grade (grades are 0-3) -> trips the grade > 3 oracle
    f.write("bad apple male 21 7 CS\n")


def main():
    print(f"generating BigSift datasets ({ROWS:,} rows each) in {HERE} …")
    for name, gen in [("airport.csv", airport), ("weather.csv", weather),
                      ("student.txt", student)]:
        path = os.path.join(HERE, name)
        with open(path, "w") as f:
            gen(f)
        mb = os.path.getsize(path) / 1e6
        print(f"  {name:<14} {ROWS:>10,} rows  ({mb:.1f} MB)")


if __name__ == "__main__":
    main()
