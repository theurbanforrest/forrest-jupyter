from datetime import datetime

def ig_ninja():
    result_df = get_tag_data_from_list(tag_list)
    write_results(result_df, "data/")
    print('ig_ninja() success')
    return True

def ig_ninja_init():
    import sys
    #!{sys.executable} -m pip install beautifulsoup4
    #!{sys.executable} -m pip install pyarrow
    #!{sys.executable} -m pip install pandas
    import requests
    import json
    import pandas as pd
    from datetime import datetime

def get_tag_data(tag_name):
  url = "https://www.instagram.com/explore/tags/" + tag_name + "/?__a=1"
  r2 = requests.get(url)
  r3 = json.loads(r2.text)
  return r3

def get_highlights(tag_json):
  data = tag_json['graphql']
  
  my_result = {}
  my_result['tag'] = data['hashtag']['name']
  my_result['time_str'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
  my_result['total_posts_snapshot'] = data['hashtag']['edge_hashtag_to_media']['count']
  
  return my_result
  
def get_tag_data_from_list(tag_list):
    all_results = []
    for tag_item in tag_list:
        data = get_highlights(get_tag_data(tag_item))
        all_results.append(data)
    print(all_results)
    return all_results

def write_results(results, directory):
    df = pd.DataFrame(results, columns=results[0].keys())
    file_name = "my-data_" + datetime.now().strftime('%Y-%m-%d_%H-%M') + ".parquet"
    df.to_parquet(directory + file_name)

def read_all(directory):
    return pd.read_parquet(directory)


ig_ninja_init()
x = get_tag_data_from_list(['berlin'])
write_results(x, './data')
