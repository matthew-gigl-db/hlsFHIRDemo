"""
 Class to handle writing to endpoints
"""

import requests

class RedoxApiRequest:
    def __init__(self, auth, base_url):
        self.auth = auth
        self.base_url = base_url

    def make_request(self, http_method, resource, action, data=None):
        response = getattr(requests, http_method)(f"{self.base_url}{resource}/{action}", data=data, auth=self.auth)
        
        return {'request':
                {
                    'http_method': http_method,
                    'url': f"{self.base_url}{resource}/{action}",
                    'data': ('' if data is None else data)
                },
                'response': {
                    'response_status_code': response.status_code, 
                    'response_time_seconds': (response.elapsed.microseconds / 1000000),
                    'response_headers': response.headers,
                    'response_text': response.text,
                    'response_url': response.url
                }
            }
        