"""
Class to handle authentication
"""

import datetime, jwt, requests, json, zoneinfo
from uuid import uuid4

class RedoxApiAuth(requests.auth.AuthBase):
  def __init__(self, 
               client_id, 
               private_key, 
               auth_json,
               source_id,
               auth_location = 'https://api.redoxengine.com/v2/auth/token'):
    self.__client_id = client_id
    self.__private_key = private_key
    self.__auth_json = json.loads(auth_json)
    self.auth_location = auth_location
    self.source_id = source_id
    self.__token = None
    self.token_expiry = None

  def get_token(self,
                now = None,
                expiration = None,
                timeout=30):
    now = (now if now is not None else datetime.datetime.now(zoneinfo.ZoneInfo("America/New_York")))
    expiration = (expiration if expiration is not None else datetime.datetime.now(zoneinfo.ZoneInfo("America/New_York")) + datetime.timedelta(minutes=5))
    if self.__token is None or now >= self.token_expiry:
      t = self.generate_token(now, expiration, timeout)
      t.raise_for_status()
      self.__token = json.loads(t.text)
      self.token_expiry = expiration
    return self.__token
  
  def __call__(self, r):
    r.headers['Authorization'] = 'Bearer %s' % self.get_token()['access_token']
    r.headers['Redox-Source-Id'] = self.source_id
    return r

  """
    Provide authentication to Redox's API and return valid token
      @param expiration = the datetime when the token expires, default 5 minutes
      @param timeout = seconds to timeout request, default 30 
  """
  def generate_token(self,
                     now = None,
                     expiration = None,
                     timeout=30):
    now = (now if now is not None else datetime.datetime.now(zoneinfo.ZoneInfo("America/New_York")))
    expiration = (expiration if expiration is not None else datetime.datetime.now(zoneinfo.ZoneInfo("America/New_York")) + datetime.timedelta(minutes=5))
    return requests.post(self.auth_location, 
        data= {
        'grant_type': 'client_credentials',
        'client_assertion_type': 'urn:ietf:params:oauth:client-assertion-type:jwt-bearer',
        'client_assertion': jwt.encode(
           {
              'iss': self.__client_id,
              'sub': self.__client_id,
              'aud': self.auth_location,
              'exp': int(expiration.timestamp()),
              'iat': int(now.timestamp()),
              'jti': uuid4().hex,
          },
          self.__private_key,
          algorithm=self.__auth_json['alg'],
          headers={
            'kid': self.__auth_json['kid'],
            'alg': self.__auth_json['alg'],
            'typ': 'JWT',
          })
      }, timeout=timeout)
    
  def can_connect(self):
    return (self.generate_token().status_code == 200)