import pandas as pd
import numpy as np
import json

def mutate_keys(obj):
    val = obj.split(':')
    if 'https' in obj:
        val = val[1:]
    else:
        val = val[:-1]

    return '_'.join(val).replace("'", "").strip()


def fix_meta_types(df):
    metas = df['meta']
    new_metas = []
    for i in range(len(metas)):
        pairs = metas.iloc[i].replace('{', '').replace('}', '').split(',')
        values = [x.split(':')[-1].replace("'", "").strip() for x in pairs]
        keys = [mutate_keys(x) for x in pairs]
        obj = {}

        for j in range(len(keys)):
           obj[keys[j]] = values[j]

        new_metas.append(obj)

    df['meta'] = new_metas

    return df

def fix_details(df):
    details = df['details']
    statuses = [''] * len(details)
    origins = [''] * len(details)
    years = [''] * len(details)
    types = [''] * len(details)


    for i in range(len(details)):
        pairs = details.iloc[i].split(',')
        keys = []
        values = []
        type = details.iloc[i].split('[')

        if len(type) > 1:
            t = details.iloc[i].split('[')[1].replace(']', '')
            types_array = t.split(',')
            types[i] = ','.join(types_array)

        for j in range(len(pairs)):
            if len(pairs[j].split(':')) > 1:
                keys.append(pairs[j].split(':')[0].replace('{', '').replace('}', '').replace("'", "").strip())
                values.append(pairs[j].split(':')[1].replace('{', '').replace('}', '').replace("'", "").strip())

        statuses[i] = values[0]
        origins[i] = values[1]
        years[i] = values[2]

    df['status'] = statuses
    df['origin'] = origins
    df['year'] = years
    df['type'] = types
    df = df.drop('details', axis=1)
    return df


def to_dict(j):
    try:
        return json.loads(j.replace('"', "").replace("'", '"'))
    except:
        return {}


def to_array_of_strings(s):
    if type(s) != 'string':
        return []

    if len(s) == 0:
        return []
    else:
        return s.replace('[', '').replace(']', '').replace("'", '').split(',')


def columns_to_type(file_name):
    d = pd.read_csv(file_name)
    d['title'] = d['title'].astype('string')
    d['url'] = d['url'].astype('string')
    d['template_image_url'] = d['template_image_url'].astype('string')

    # meta names fixed and to dictionary
    d = fix_meta_types(d)

    # details to separate columns in dataframe
    d = fix_details(d)

    # content to dict
    d['content'] = [to_dict(x) for x in d['content']]
    # tags to array of strings
    d['tags'] = [to_array_of_strings(x) for x in d['tags']]
    # additional references to dictionary
    d['additional_references'] = [to_dict(x) for x in d['additional_references']]
    # search keywords to array of strings
    d['search_keywords'] = [to_array_of_strings(x) for x in d['search_keywords']]
    # parent - string, jätame
    # siblings - to array of strings
    d['siblings'] = [to_array_of_strings(x) for x in d['siblings']]
    # children - to array of strings
    d['children'] = [to_array_of_strings(x) for x in d['children']]

    return d


data = columns_to_type('1636050615_filtered.csv')

print(data.dtypes)

print(type(data.iloc[1].meta))
print(data.siblings)
