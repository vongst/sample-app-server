from os import write
import ndjson
import json
from collections import defaultdict

input_file_path  = "./data/data_6.data"

# load from file-like objects
with open(input_file_path) as f:
    data = ndjson.load(f)

def write_to_json(content=""):
    with open(input_file_path + ".out", 'w', encoding='utf-8') as f:
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
    attr = {}

    for e in attr_arr: 
        if "user_id" in e:
            uid = int(e["user_id"])

            if uid not in attr: 

                attributes = {}
                last_updated = {}

                for key in e["data"]:
                    try :
                        attributes[key] = e["data"][key]
                        last_updated[key] = e["timestamp"]
                        
                    except KeyError as error:
                        pass
                        # print(error)

                attr[int(uid)] = {
                    "attributes": attributes,
                    "attr_last_updated": last_updated
                }
                
                    
            # elif uid in attr:
            #     for key in e["data"]:

            #         if key in attr[int(uid)]["attributes"]: 
            #             if e["timestamp"] > attr[int(uid)]["attr_last_updated"][key]: 
            #                 attr[int(uid)]["attributes"][key] = e["data"][key]
            #                 attr[int(uid)]["attr_last_updated"][key] = e["timestamp"]

            #             pass
            #         elif key not in key in attr[int(uid)]["attributes"]:
            #             attr[int(uid)]["attributes"][key] = e["data"][key]
            #             attr[int(uid)]["attr_last_updated"][key] = e["timestamp"]


    return attr

def merge_events_on_attr(attr: dict, events: dict) -> dict:
    for uid in events: 
        attr[uid]["events"] = events[uid]
    
    return attr
    pass

def format(attr: dict) -> dict:
    customers = []
    for uid in attr: 
        attr[uid]["id"] = int(uid)
        attr[uid]["last_updated"] = max(attr[uid]["attr_last_updated"].values())
        # try: 
        #     del attr[uid]["attr_last_updated"]
        # except: 
        #     pass
        customers.append(attr[uid])
    
    return customers
    pass

attributes, events = attributes_events(data)
events = get_events_dict(events)
attr = get_attributes_dict(attributes)
customers = merge_events_on_attr(attr, events)

formatted = format(customers)
write_to_json(formatted)