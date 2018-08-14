import requests
import time
import mimetypes
import json
import logging
from requests.models import PreparedRequest

class HttpLib:
    max_retries = 3
    sleep_duration = 10
    def __init__(self, logbook_logger = None):
        self.logbook_logger = logbook_logger

    def error_log(self, message):
        if self.logbook_logger:
            self.logbook_logger.error(message)
        else:
            print message


    def get(self, endpoint, args=None, mode=None):
        return self._request('GET', endpoint, args, mode)

    def post(self, endpoint, args, data=None, files=None, filename=None, mode=None):
        return self._request('POST', endpoint, args, mode, data, files, filename)

    def delete(self, endpoint, args, data):
        return self._request('DELETE', endpoint, args, None, data)


    def _request(self, method, endpoint, args=None, mode=None, data=None, files=None, filename=None):
        retries = 1
        while True:
            if retries > self.max_retries:
                return None
            try:
                headers = {'Accept-Encoding': 'gzip'}
                pr = PreparedRequest()
                pr.prepare_url(endpoint, args)

                if method == 'POST':
                    if mode is 'nojsondumps':
                        r = requests.post(endpoint, params=args, data=data)
                    elif files is None:
                        headers['Content-type'] = 'application/json'
                        r = requests.post(endpoint, params=args, json=data, headers=headers)
                    else:
                        mimetype = mimetypes.guess_type(files)[0]
                        file = {filename: (files, open(files, 'rb'), mimetype)}
                        r = requests.post(endpoint, params=args, json=data, files=file)
                elif method == 'DELETE':
                    headers = {'Content-type': 'application/json'}
                    r = requests.delete(endpoint, params=args, json=data, headers=headers)
                else:
                    if len(pr.url) > 7000:
                        args['_method'] = 'GET'
                        r = requests.post(endpoint, data=args, headers=headers)
                    else:
                        r = requests.get(endpoint, params=args, headers=headers)

                requests_log = logging.getLogger("requests.packages.urllib3")
                requests_log.debug('content: %s', r.content)

                if mode is 'nojson':
                    return r
                else:
                    r_json = r.json()
                    # faking 606
                    #if '/identity/oauth/token' not in endpoint:
                    #    r_json = json.loads("{\"requestId\":\"5576#16523a39ed7\",\"success\":false,\"errors\":[{\"code\":\"606\",\"message\":\"Max rate limit '100' exceeded with in '20' secs\"}]}")
                    # faking HTTP exception
                    #raise requests.exceptions.Timeout('test timeout exception')

                    # faking other exception
                    #raise Exception('test exception')
                    # if we still hit the rate limiter, do not return anything so the call will be retried
                    if 'success' in r_json:  # this is for all normal API calls (but not the access token call)
                        if r_json['success'] == False:
                            self.error_log('error from http_lib.py: ' + str(r_json['errors'][0]))
                            if r_json['errors'][0]['code'] in ('606', '615', '604'):
                                # this handles Marketo exceptions; HTTP response is still 200, but error is in the JSON
                                error_code = r_json['errors'][0]['code']
                                error_description = {
                                    '606': 'rate limiter',
                                    '615': 'concurrent call limit',
                                    '604': 'timeout'}
                                if retries < self.max_retries:
                                    self.error_log('Attempt %s. Error %s, %s. Pausing, then trying again.' % (retries, error_code, error_description[error_code]))
                                    time.sleep(self.sleep_duration * (retries*6 - 5)) # sleep much longer if two rate/timeouts in a row
                                else:
                                    self.error_log('Attempt %s. Error %s, %s. This was the final attempt.' % (retries, error_code, error_description[error_code]))
                                    return r_json
                                retries += 1
                            else:
                                # fatal exceptions will still error out; exceptions caught above may be recoverable
                                return r_json
                        else:
                            return r_json
                    else:
                        return r_json  # this is only for the access token call
            except requests.exceptions.RequestException as e:
                if retries < self.max_retries:
                    self.error_log(u"Retrying after exception: {}: {}".format(type(e).__name__,
                                                                              e.message if e.message else str(e)))
                    time.sleep(self.sleep_duration * (retries * 6 - 5))
                else:
                    raise e
            except Exception as e:
                raise e


