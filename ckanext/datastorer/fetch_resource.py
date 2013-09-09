from datetime import datetime
import hashlib
import httplib
import json
import logging as log
import os
import requests
import tempfile
import urllib
import urlparse
import ckan.logic as logic


HTTP_ERROR_CODES = {
    httplib.MULTIPLE_CHOICES: "300 Multiple Choices not implemented",
    httplib.USE_PROXY: "305 Use Proxy not implemented",
    httplib.INTERNAL_SERVER_ERROR: "Internal server error on the remote "
                                   "server",
    httplib.BAD_GATEWAY: "Bad gateway",
    httplib.SERVICE_UNAVAILABLE: "Service unavailable",
    httplib.GATEWAY_TIMEOUT: "Gateway timeout",
    httplib.METHOD_NOT_ALLOWED: "405 Method Not Allowed"
}


class DownloadError(Exception):
    pass


class ChooseNotToDownload(Exception):
    pass


class ResourceNotModified(Exception):
    pass


class LinkCheckerError(Exception):
    pass


class LinkInvalidError(LinkCheckerError):
    pass


class LinkHeadRequestError(LinkCheckerError):
    pass


class CkanError(Exception):
    pass


def _clean_content_type(ct):
    # For now we should remove the charset from the content type and
    # handle it better, differently, later on.
    if 'charset' in ct:
        return ct[:ct.index(';')]
    return ct


def download(context, resource, max_content_length, data_formats,
             url_timeout=30, check_modified=False):
    '''Given a resource, tries to download it.

    If the size or format is not acceptable for download then
    ChooseNotToDownload is raised.

    If there is an error performing the download then
    DownloadError is raised.
    '''

    url = resource['url']

    if (resource.get('resource_type') == 'file.upload' and
            not url.startswith('http')):
        url = context['site_url'].rstrip('/') + url

    link_context = "{}"
    link_data = json.dumps({
        'url': url,
        'url_timeout': url_timeout
    })

    headers = json.loads(link_checker(link_context, link_data))

    # check to see if remote resource has been modified since the CKAN
    # resource was last updated
    #
    # note: checks by hashes
    remote_last_mod = headers.get('last-modified')
    # If the server doesn't return a last-modifed, we shouldn't get stuck
    # in a loop saving a sha1 of None and comparing that
    remote_last_mod_hash = None
    if remote_last_mod is not None:
        remote_last_mod_hash = hashlib.sha1(remote_last_mod).hexdigest()

    try:
        resource_hash = json.loads(resource.get('hash'))
        resource_content_hash = resource_hash['content']
        resource_header_hash = resource_hash['header']
        if check_modified and (remote_last_mod_hash is not None and
                               resource_header_hash == remote_last_mod_hash):
            raise ResourceNotModified(
                'Resource {0} not modified'.format(resource['id']))
    except ValueError:
        resource_content_hash = resource.get('hash')
        resource_header_hash = None
    except ResourceNotModified:
        raise ResourceNotModified
    except:
        resource_content_hash = None
        resource_header_hash = None

    resource_format = resource['format'].lower()
    ct = _clean_content_type(headers.get('content-type', '').lower())
    cl = headers.get('content-length')

    resource_changed = False

    if resource.get('mimetype') != ct:
        resource_changed = True
        resource['mimetype'] = ct

    # this is to store the size in case there is an error, but the real size
    # check is done after dowloading the data file, with its real length
    if cl is not None and (resource.get('size') != cl):
        resource_changed = True
        resource['size'] = cl

    # make sure resource content-length does not exceed our maximum
    if cl and int(cl) >= max_content_length:
        if resource_changed:
            _update_resource(context, resource)
        # record fact that resource is too large to archive
        log.warning('Resource too large to download: %s > max (%s). '
                    'Resource: %s %r', cl, max_content_length, resource['id'],
                    url)
        raise ChooseNotToDownload("Content-length %s exceeds maximum allowed "
                                  "value %s" % (cl, max_content_length))

    # check that resource is a data file
    if data_formats != 'all' and not (resource_format in data_formats or
                                      ct.lower() in data_formats):
        if resource_changed:
            _update_resource(context, resource)
        log.warning('Resource wrong type to download: %s / %s. Resource: %s '
                    '%r', resource_format, ct.lower(), resource['id'], url)
        raise ChooseNotToDownload('Of content type "%s" which is not a '
                                  'recognised data file for download' % ct)

    # get the resource and archive it
    try:
        res = requests.get(url, timeout=url_timeout)
    except requests.exceptions.ConnectionError, e:
        raise DownloadError('Connection error: %s' % e)
    except requests.exceptions.HTTPError, e:
        raise DownloadError('Invalid HTTP response: %s' % e)
    except requests.exceptions.Timeout, e:
        raise DownloadError('Connection timed out after %ss' % url_timeout)
    except requests.exceptions.TooManyRedirects, e:
        raise DownloadError('Too many redirects')
    except requests.exceptions.RequestException, e:
        raise DownloadError('Error downloading: %s' % e)
    except Exception, e:
        raise DownloadError('Error with the download: %s' % e)

    length, hash, saved_file = _save_resource(resource, res,
                                              max_content_length)

    # check if resource size changed
    if unicode(length) != resource.get('size'):
        resource_changed = True
        resource['size'] = unicode(length)

    # check that resource did not exceed maximum size when being saved
    # (content-length header could have been invalid/corrupted, or not accurate
    # if resource was streamed)
    #
    # TODO: remove partially archived file in this case
    if length >= max_content_length:
        if resource_changed:
            _update_resource(context, resource)
        # record fact that resource is too large to archive
        log.warning('Resource found to be too large to archive: %s > max (%s).'
                    ' Resource: %s %r', length, max_content_length,
                    resource['id'], url)
        raise ChooseNotToDownload("Content-length after streaming reached "
                                  "maximum allowed value of %s"
                                  % max_content_length)

    # zero length usually indicates a problem too
    if length == 0:
        if resource_changed:
            _update_resource(context, resource)
        # record fact that resource is zero length
        log.warning('Resource found was zero length - not archiving. '
                    'Resource: %s %r', resource['id'], url)
        raise DownloadError("Content-length after streaming was zero")

    # update the resource metadata in CKAN if the resource has changed
    if resource_content_hash != hash or (resource_header_hash is None
                                         and remote_last_mod_hash is not None):
        resource['hash'] = json.dumps(
            {'content': hash, 'header': remote_last_mod_hash})
        try:
            # This may fail for archiver.update() as a result of the resource
            # not yet existing, but is necessary for dependant extensions.
            _update_resource(context, resource)
        except:
            pass

    log.warning('Resource downloaded: id=%s url=%r cache_filename=%s length=%s'
                ' hash=%s', resource['id'], url, saved_file, length, hash)

    return {'length': length,
            'hash': resource_content_hash,
            'headers': headers,
            'saved_file': saved_file}


def link_checker(context, data):
    """
    Check that the resource's url is valid, and accepts a HEAD request.

    Raises LinkInvalidError if the URL is invalid
    Raises LinkHeadRequestError if HEAD request fails

    Returns a json dict of the headers of the request
    """
    data = json.loads(data)
    url_timeout = data.get('url_timeout', 30)

    error_message = ''
    headers = {}

    # Find out if it has unicode characters, and if it does, quote them
    # so we are left with an ascii string
    url = data['url']
    try:
        url = url.decode('ascii')
    except:
        parts = list(urlparse.urlparse(url))
        parts[2] = urllib.quote(parts[2].encode('utf-8'))
        url = urlparse.urlunparse(parts)
    url = str(url)

    # parse url
    parsed_url = urlparse.urlparse(url)
    # Check we aren't using any schemes we shouldn't be
    allowed_schemes = ['http', 'https', 'ftp']
    if not parsed_url.scheme in allowed_schemes:
        raise LinkInvalidError("Invalid url scheme")
    # check that query string is valid
    # see: http://trac.ckan.org/ticket/318
    # TODO: check urls with a better validator?
    #       eg: ll.url (http://www.livinglogic.de/Python/url/Howto.html)?
    elif any(['/' in parsed_url.query, ':' in parsed_url.query]):
        raise LinkInvalidError("Invalid URL")
    else:
        # Send a head request
        try:
            res = requests.head(url, timeout=url_timeout)
            headers = res.headers
        except httplib.InvalidURL, ve:
            log.warning("Could not make a head request to %r, error is: %s. "
                        "Package is: %r. This sometimes happens when using an "
                        "old version of requests on a URL which issues a 301 "
                        "redirect. Version=%s", url, ve, data.get('package'),
                        requests.__version__)
            raise LinkHeadRequestError("Invalid URL or Redirect Link")
        except ValueError, ve:
            log.warning("Could not make a head request to %r, error is: %s. "
                        "Package is: %r.", url, ve, data.get('package'))
            raise LinkHeadRequestError("Could not make HEAD request")
        except requests.exceptions.ConnectionError, e:
            raise LinkHeadRequestError('Connection error: %s' % e)
        except requests.exceptions.HTTPError, e:
            raise LinkHeadRequestError('Invalid HTTP response: %s' % e)
        except requests.exceptions.Timeout, e:
            raise LinkHeadRequestError('Connection timed out after %ss'
                                       % url_timeout)
        except requests.exceptions.TooManyRedirects, e:
            raise LinkHeadRequestError('Too many redirects')
        except requests.exceptions.RequestException, e:
            raise LinkHeadRequestError('Error during request: %s' % e)
        except Exception, e:
            raise LinkHeadRequestError('Error with the request: %s' % e)
        else:
            if not res.ok or res.status_code >= 400:
                if res.status_code in HTTP_ERROR_CODES:
                    error_message = ('Server returned error: %s'
                                     % HTTP_ERROR_CODES[res.status_code])
                else:
                    error_message = ("URL unobtainable: Server returned "
                                     "HTTP %s" % res.status_code)
                raise LinkHeadRequestError(error_message)
    return json.dumps(headers)


def _update_resource(context, resource):
    """
    Use CKAN API to update the given resource.
    Returns the content of the response.

    """
    resource['last_modified'] = datetime.now().isoformat()
    try:
        logic.get_action('resource_update')(context, resource)
    except Exception as e:
        log.exception(e)
        raise CkanError('ckan failed to update resource')


def _save_resource(resource, response, max_file_size, chunk_size=1024*16):
    """
    Write the response content to disk.

    Returns a tuple:

        (file length: int, content hash: string, saved file path: string)
    """
    resource_hash = hashlib.sha1()
    length = 0

    fd, tmp_resource_file_path = tempfile.mkstemp()

    with open(tmp_resource_file_path, 'wb') as fp:
        for chunk in response.iter_content(chunk_size=chunk_size,
                                           decode_unicode=False):
            fp.write(chunk)
            length += len(chunk)
            resource_hash.update(chunk)

            if length >= max_file_size:
                break

    os.close(fd)

    content_hash = unicode(resource_hash.hexdigest())
    return length, content_hash, tmp_resource_file_path
