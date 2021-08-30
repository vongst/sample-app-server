from os import write
import ndjson
import json
from collections import defaultdict
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("file", help="path to input ndjson file")
args = parser.parse_args()

# load from file-like objects
with open(args.file) as f:
    data = ndjson.load(f)

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

def get_events_dict(events_arr: list) -> dict: 
    events = defaultdict(dict)

    for e in events_arr: 
        uid = int(e["user_id"])
        
        if e["name"] not in events[uid]:
            events[uid][e["name"]] = 1
        else:
            events[uid][e["name"]] += 1 

    return events

def get_attributes_dict(attr_arr: list) -> dict: 
    attr = defaultdict(dict)

    for item in attr_arr: 
        uid = int(item["user_id"])

        if "attributes" not in attr[uid]:
            attr[uid]["attributes"] = item["data"]
            attr[uid]["timestamps"] = [item["timestamp"]]
        elif "attributes" in attr[uid]:
            attr[uid]["attributes"].update(item["data"])
            attr[uid]["timestamps"].append(item["timestamp"])

    return attr

def get_formatted(attr: dict, events: dict) -> dict:
    customers = []

    for uid in events: 
        attr[uid]["events"] = events[uid]
        attr[uid]["id"] = int(uid)
        attr[uid]["last_updated"] = int(attr[uid]["attributes"]["created_at"])

        customers.append(attr[uid])
    
    return customers

attributes, events = split_attributes_and_events(data)

events = get_events_dict(events)
attr = get_attributes_dict(attributes)
formatted = get_formatted(attr, events)
write_to_json(formatted)