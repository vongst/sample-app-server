from os import write
import ndjson
import json
from collections import defaultdict
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("file", help="path to input ndjson file")
args = parser.parse_args()

def write_to_json(content=""):
    with open(args.file + ".out", 'w', encoding='utf-8') as f:
        json.dump(content, f, ensure_ascii=False, indent=4)


# Split initial ndjson file by event type
# assuming 2 event types
# remove any without user id

def split_attributes_and_events(data: list) -> tuple:
    attr, events = [], []
    for d in data: 
        if "user_id" in d:
            (attr, events)[d["type"] == "event"].append(d)

    return attr, events

def format_events(events_arr: list) -> dict: 
    events = defaultdict(dict)

    for e in events_arr: 
        uid = int(e["user_id"])
        if uid not in events:
            ev = events[uid]["events"] = defaultdict(dict)
        if e["name"] not in ev:
            ev[e["name"]] = 1
        else:
            ev[e["name"]] += 1 

    return events

def format_attr(attr_arr: list) -> dict: 
    attr = defaultdict(dict)

    for a in attr_arr: 
        uid = int(a["user_id"])

        if "attributes" not in attr[uid]:
            attr[uid]["attributes"] = a["data"]
            attr[uid]["timestamps"] = [a["timestamp"]]
        elif "attributes" in attr[uid]:
            attr[uid]["attributes"].update(a["data"])
            attr[uid]["timestamps"].append(a["timestamp"])

    return attr

with open(args.file) as f:
    data = ndjson.load(f)

attr, events = split_attributes_and_events(data)
attr    = format_attr(attr)
events  = format_events(events)

# merge events dict onto attributes dict
write_to_json([{**events, **attr}])