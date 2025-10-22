# bookroad/services/aladin_api.py
import requests
from django.conf import settings

class AladinAPI:
    BASE_URL = "http://www.aladin.co.kr/ttb/api"

    def __init__(self):
        self.ttb_key = settings.ALADIN_TTB_KEY

    def _make_request(self, endpoint, params):
        default_params = {
            'TTBKey': self.ttb_key,
            'Output': 'JS',
            'Version': '20131101'
        }
        all_params = {**default_params, **params}

        try:
            response = requests.get(f"{self.BASE_URL}/{endpoint}", params=all_params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Aladin API Error: {e}")
            return None

    # [변경점 1] 특정 인자 이름 대신 **kwargs를 사용하여 모든 키워드 인자를 받도록 변경
    def item_search(self, **kwargs):
        """키워드로 상품을 검색합니다."""
        return self._make_request('ItemSearch.aspx', kwargs)

    # [변경점 2] 여기도 동일하게 **kwargs로 변경
    def item_list(self, **kwargs):
        """신간, 베스트셀러 등 특정 리스트를 조회합니다."""
        return self._make_request('ItemList.aspx', kwargs)

    def item_lookup(self, **kwargs):
        """ISBN으로 특정 상품의 상세 정보를 조회합니다."""
        return self._make_request('ItemLookUp.aspx', kwargs)