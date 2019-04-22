import requests

import utils.common
import urllib.parse
from config import load_config


class RESTClient(object):
    def __init__(self, config=load_config(), token=None, verbose=False):
        self.config = config
        self.endpoint = config["api"]["aminer"]["endpoint"]
        self.endpoint_v2 = config["api"]["aminer_v2"]["endpoint"]
        self.verbose = verbose
        if token:
            self.token = token
        else:
            self.token = config["api"]["token"]

    def get_url(self, url, version=1):
        if version == 2:
            return self.endpoint_v2
        full_url = self.endpoint + url
        print(full_url)
        return full_url

    def get_person(self, id):
        resp = utils.common.rest_get(self.get_url("person/%s" % id))
        return resp.json()

    def get_experts(self, ebid, offset, size):
        resp = utils.common.rest_get(
            self.get_url("roster/%s/order-by/h_index/offset/%s/size/%s" % (ebid, offset, size)))
        experts = resp.json()
        return experts

    def get_all_experts(self, ebid, size=50):
        for item in utils.common.iterate_pages(self.get_experts, ebid, is_list=False, size=size, k_data="result",
                                               k_total="total"):
            yield item

    def get_institution(self, iid):
        resp = utils.common.rest_get(self.get_url("aff/summary/%s" % iid))
        return resp.json()

    def get_ins_members(self, iid, offset=0, size=0):
        resp = utils.common.rest_get(self.get_url("roster/%s/order-by/h_index/offset/%s/size/%s" % (iid, offset, size)))
        return resp.json()

    def get_ins_members_all(self, iid):
        for item in utils.common.iterate_pages(self.get_ins_members, iid, k_data="result", k_total="total",
                                               is_list=False):
            yield item

    def get_ins_pubs(self, iid, offset=0, size=0):
        user_agent = {'User-agent': 'Mozilla/5.0'}
        resp = requests.get(self.get_url("search/pub/inst/%s?offset=%s&size=%s" % (iid, offset, size)),
                            headers=user_agent)
        return resp.json()

    def search_person(self, query, offset=0, size=30):
        user_agent = {'User-agent': 'Mozilla/5.0'}
        resp = requests.get(self.get_url("search/person?query=%s&offset=%s&size=%s" % (urllib.parse.quote_plus(query), offset, size)),
                            headers=user_agent)
        return resp.json()

    def search_person_advanced(self, name="", org="", term="", offset=0, size=20, sort="relevance", filters=None):
        url = "search/person/advanced?name=%s&org=%s&term=%s&offset=%s&size=%s&sort=%s&term=" % (urllib.parse.quote_plus(name), org, urllib.parse.quote_plus(term), offset, size, sort)
        if filters:
            filter_query = "&".join(["{}={}".format(k, v) for k,v in filters.items()])
            url += "&" + filter_query
        resp = utils.common.rest_get(self.get_url(url))
        return resp.json()

    def search_pub_advanced(self, query, offset=0, size=20):
        resp = utils.common.rest_get(
            self.get_url(
                "search/pub/advanced?escape={}&name={}&offset={}&org={}&size={}&term={}&sort={}".format(
                    query.get('escape', ''),
                    query.get('name', ''),
                    offset, query.get('org', ''), size,
                    urllib.parse.quote_plus(query.get('term', '')), query.get('sort', '')
                )
            )
        )
        return resp.json()

    def search_activity(self, query, offset=0, size=20):
        resp = utils.common.rest_get(
            self.get_url(
                "search/activity?escape={}&name={}&offset={}&org={}&size={}&term={}&sort={}".format(
                    query.get('escape', ''),
                    query.get('name', ''),
                    offset, query.get('org', ''), size,
                    urllib.parse.quote_plus(query.get('term', '')), query.get('sort', '')
                )
            )
        )
        return resp.json()

    def search_pub_advanced_all(self, term="", name="", org="", sort="", size=20):
        query = {
            'term': term,
            'name': name,
            'org': org,
            'sort': sort
        }
        for item in utils.common.iterate_pages(self.search_pub_advanced, urllib.parse.quote_plus(query), is_list=False, size=size, k_data="result", k_total="total"):
            yield item

    def get_ins_all_pubs(self, iid, size=50):
        offset = 0
        pubs = []
        while True:
            data = self.get_ins_pubs(iid, offset, size)
            if data["size"] < offset:
                break
            pubs.extend(data["result"])
            offset += size
        return pubs

    def get_ins_pubs_1(self, iid, offset=0, size=0):
        # https://api.aminer.org/api/search/pub/inst/575f9e5976d91118a4b492b1?offset=0&size=50
        user_agent = {'User-agent': 'Mozilla/5.0'}
        # resp = requests.get(self.get_url("search/pub/inst/%s?offset=%s&size=%s" % (iid, offset, size)), headers=user_agent)
        resp = requests.get(self.get_url("aff/pubs/iid/%s/all/cite/offset/%s/size/%s" % (iid, offset, size)),
                            headers=user_agent)
        return resp.json()

    def get_ins_all_pubs_1(self, iid, size=30):
        offset = 0
        pubs = []
        cnt = 0
        while True:
            data = self.get_ins_pubs_1(iid, offset, size)
            if data["size"] < offset:
                break
            pubs.extend(data["data"])
            cnt += len(data["data"])
            print(offset, data["size"], cnt, len(data["data"]))
            offset += size
        return pubs

    def get_ins_projects(self, iid, offset=0, size=0):
        resp = utils.common.rest_get(self.get_url("aff/projects/iid/%s/all/offset/%s/size/%s" % (iid, offset, size)))
        return resp.json()

    def get_ins_patents(self, iid, offset=0, size=0):
        resp = utils.common.rest_get(self.get_url("aff/patents/iid/%s/all/offset/%s/size/%s" % (iid, offset, size)))
        return resp.json()

    def get_person_pubs(self, pid, offset=0, size=0):
        resp = utils.common.rest_get(self.get_url("person/pubs/%s/all/year/%s/%s" % (pid, offset, size)))
        return resp.json()

    def get_person_pub_all(self, pid, total):
        for item in utils.common.iterate_pages(self.get_person_pubs, pid, total=total):
            yield item

    def get_ins_pubs_by_label(self, iid, label, offset=0, size=0):
        resp = utils.common.rest_get(
            self.get_url(
                "search/pub/inst/%s?labels=%s&offset=%s&size=%s" % (iid, label, offset, size)
                # "aff/pubs/tag/%s/iid/57d2043c2abe00ce522da340/all/offset/%s/size/%s" % (label, offset, size)
            )
        )
        return resp.json()

    def get_ins_pubs_by_label_1(self, iid, label, offset=0, size=0):
        user_agent = {'User-agent': 'Mozilla/5.0'}
        resp = utils.common.rest_get(
            self.get_url(
                # "search/pub/inst/552e0af6dabfae7de9e77cf6?labels=%s&offset=%s&size=%s" % (label, offset, size)
                "aff/pubs/tag/%s/iid/%s/all/offset/%s/size/%s" % (label, iid, offset, size)
            ))
        return resp.json()

    def get_pubs_by_label_all(self, label):
        for item in utils.common.iterate_pages(self.get_ins_pubs_by_label, label, k_data="result", k_total="size",
                                               is_list=False):
            yield item

    def assign_pub_to_claim(self, aid, pid, order, score):
        resp = utils.common.rest_post(self.get_url("person/pubs/%s/tobeconfirmed/%s/%s/%s?token=iamkegger" % (str(aid), str(pid), order, score)), token=self.token).json()
        if "status" in resp and resp["status"]:
            return True
        else:
            print("error", aid, pid, resp)
            return False

    def get_pub_by_author(self, payload, offset, size):
        aid, name, stype = payload
        resp = utils.common.rest_get(
            self.get_url(
                "search/pub/advanced?escape=%s&name=%s&offset=%s&org=&size=%s&term=&stype=%s"
                % (aid, name, offset, size, stype)
            )
        )
        return resp.json()

    def get_pub_by_author_all(self, aid, name, stype=""):
        for item in utils.common.iterate_pages(self.get_pub_by_author, (aid, name, stype), is_list=False, k_data="result", k_total="total", size=50):
            yield item

    def check_pos(self, aid, pid):
        resp = utils.common.rest_get(
            self.get_url("person/pos_check/%s/%s" % (aid, pid))
        )
        try:
            data = resp.json()
            if data["status"]:
                return data["pos"]
            else:
                return None
        except Exception as e:
            print(e)
            return None

    def replace_roster(self, rid, pid, opid):
        data = {"id": rid,"nid": pid, "oid": opid}
        resp = utils.common.rest_put(self.get_url("roster/%s/u/n/%s/o/%s" % (rid, pid, opid)), data)
        return resp.json()

    def refresh_person_stats(self, aids):
        resp = utils.common.rest_post(self.endpoint_v2, data=[{
            "action": "person.UpdatePersonIndices",
            "parameters": {
                "ids": [str(aid) for aid in aids] if type(aids) is list else [str(aids)]
            }
        }], token=self.token)
        print(resp.text)

    def get_trend(self, key):
        resp = utils.common.rest_get("https://dc_api.aminer.cn/trend/%s" % key)
        return resp.json()

    def get_term_freq(self, key):
        resp = utils.common.rest_get("https://api.aminer.cn/api/topic/summary/m/%s" % key)
        return resp.json()

    def get_terms_freq(self, terms, start_year, end_year):
        resp = utils.common.rest_post(self.endpoint_v2, data=[{
            "action": "terms.GetTerm",
            "parameters": {
                "terms": terms,
                "years": [start_year, end_year],
                "is_fromto": True
            }
        }], token=self.token).json()
        return resp["data"][0]["items"]

    def search_pub_by_names(self, names, exclude_ids=(), start=2000, end=2030):
        resp = utils.common.rest_post(self.endpoint_v2, data=[{
            "action": "publication.GetPubsByAuthorNames",
            "parameters": {
                "names": names,
                "startYear": start,
                "endYear": end,
                "exclude_ids": exclude_ids
            },
            "schema": {
                "publication": ["id", "year", "authors.name",  "authors.org", "title", "num_citation"]
            }
        }], token=self.token).json()
        return resp["data"][0].get("items", [])

    def search_person_by_coauthor(self, source, target):
        resp = utils.common.rest_post(self.endpoint_v2, data=[{
            "action": "dm_person.SortAuthorByNumCoauthor",
            "parameters": {
               "source_name": source,
               "target_name": target
            }
        }], token=self.token).json()
        return resp["data"][0].get("items", [])

    def search_person_by_coauthor_batch(self, data):
        json_data = [{
            "action": "dm_person.SortAuthorByNumCoauthor",
            "parameters": {
                "batch": [
                    {"source_name": s, "target": [{"target_name": n, "weight": w} for (n, w) in ts]}
                    for (s, ts) in data
                ]
            }
        }]
        print(json_data)
        resp = utils.common.rest_post(self.endpoint_v2, data=json_data, token=self.token).json()
        return resp["data"][0].get("items", [])

    def get_ego_network(self, aid):
        resp = utils.common.rest_get(self.get_url("person/ego/%s?cache=true" % aid)).json()
        nodes = []
        for n in resp["nodes"]:
            if n["w"] != 0:
                nodes.append(n)
        return nodes
