import requests
import time
import mimetypes
import logging

from requests.models import PreparedRequest

from marketorestpython.helper.exceptions import MarketoException


class HttpLib:
    max_retries = 3
    sleep_duration = 20

    def __init__(self, logbook_logger=None):
        self.logbook_logger = logbook_logger

    def error_log(self, message):
        if self.logbook_logger:
            self.logbook_logger.error(message)
        else:
            print(message)

    def get(self, endpoint, args=None, mode=None, timeout=None, stream=False):
        return self._request('GET', endpoint, args, mode, timeout, stream=stream)

    def post(self, endpoint, args, data=None, files=None, filename=None, mode=None, timeout=None, stream=False):
        return self._request('POST', endpoint, args, mode, timeout, data, files, filename, stream=stream)

    def delete(self, endpoint, args, data, timeout=None):
        return self._request('DELETE', endpoint, args, None, timeout, data)

    def _request(self, method, endpoint, args=None, mode=None, timeout=None, data=None, files=None, filename=None,
                 stream=False):
        retries = 1
        while True:
            if retries > self.max_retries:
                return None
            try:
                headers = {'Accept-Encoding': 'gzip'}
                pr = PreparedRequest()
                pr.prepare_url(endpoint, args)

                if method == 'POST':
                    if mode == 'nojsondumps':
                        headers['Content-type'] = 'application/x-www-form-urlencoded; charset=utf-8'
                        r = requests.post(endpoint, params=args, data=data, headers=headers, timeout=timeout)
                    elif files is None:
                        headers['Content-type'] = 'application/json; charset=utf-8'
                        r = requests.post(endpoint, params=args, json=data, headers=headers, timeout=timeout)
                    else:
                        mimetype = mimetypes.guess_type(files)[0]
                        file = {filename: (files, open(files, 'rb'), mimetype)}
                        r = requests.post(endpoint, params=args, json=data, files=file, timeout=timeout)
                elif method == 'DELETE':
                    headers['Content-type'] = 'application/json; charset=utf-8'
                    r = requests.delete(endpoint, params=args, json=data, headers=headers, timeout=timeout)
                else:
                    if len(pr.url) > 7000:
                        args['_method'] = 'GET'
                        r = requests.post(endpoint, data=args, headers=headers, timeout=timeout)
                    else:
                        r = requests.get(endpoint, params=args, headers=headers, timeout=timeout, stream=stream)

                requests_log = logging.getLogger("requests.packages.urllib3")
                requests_log.debug('content: %s', r.content)

                if mode == 'nojson':
                    return r
                else:
                    try:
                        r_json = r.json()
                    except ValueError:  # case when request is failed with bad status code (like 502, 413, etc...)
                        self.error_log('error from http_lib.py: ' + r.text)
                        error_message = r.text
                        if r.status_code == 502:
                            error_message = 'Marketo instance is unavailable'
                        return {
                            'success': False,
                            'errors': [{
                                'code': r.status_code,
                                'message': error_message
                            }]
                        }
                    else:
                        if mode != 'accesstoken' and r_json.get('success') is False:  # this is for all normal API calls (but not the access token call)
                            self.error_log('error from http_lib.py: ' + str(r_json['errors'][0]))
                            if r_json['errors'][0]['code'] in ('606', '615', '604'):
                                # this handles Marketo exceptions; HTTP response is still 200,
                                # but error is in the JSON
                                error_code = r_json['errors'][0]['code']
                                error_description = {
                                    '606': 'rate limiter',
                                    '615': 'concurrent call limit',
                                    '604': 'timeout'}
                                if retries < self.max_retries:
                                    self.error_log('Attempt %s. Error %s, %s. '
                                                   'Pausing, then trying again.' % (retries,
                                                                                    error_code,
                                                                                    error_description[error_code]))
                                    time.sleep(self.sleep_duration * (retries*6 - 5))  # sleep much longer if two rate/timeouts in a row
                                else:
                                    self.error_log('Attempt %s. Error %s, %s. '
                                                   'This was the final attempt.' % (retries,
                                                                                    error_code,
                                                                                    error_description[error_code]))
                                    raise MarketoException(r_json['errors'][0])
                                retries += 1
                            else:
                                raise MarketoException(r_json['errors'][0])
                        else:
                            return r_json  # this is only for the access token call
            except requests.exceptions.RequestException as e:
                if retries < self.max_retries:
                    self.error_log(u"Retrying after exception: {}: {}".format(type(e).__name__, str(e)))
                    time.sleep(self.sleep_duration * (retries * 6 - 5))
                else:
                    raise e
