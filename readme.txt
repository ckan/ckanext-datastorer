CKAN Webstorer Extension
=======================

The CKAN Webstorer Extension provides a Celery task for automatically
 saving ckan resources that link to csv and excel files into the webstore.


Installation
------------

Install the plugin using pip. Download the source, then
from the ckanext-archiver directory, run

::

    $ pip install -e ./

Install Webstore located at https://github.com/okfn/ckanext-webstorer.

Add the folowing to your ckan config file, altering the url to where your webstore is located::

    ckan.webstore_url=http://0.0.0.0:5555

Make sure your webstore configuration has the following changing the CKAN_DB_URI::

    AUTH_FUNCTION = 'ckan'
    CKAN_DB_URI = 'postgresql://ckantest:pass@localhost/ckan'

Start the celery deamon.  This can be done in development by::

    paster celeryd # this is assuming a development.ini file

In production the deamon should be run with a different ini file and be run as an init script.
The simplist way to do this is to install supervisor::

    apt-get insatll supervisor

Using this file as a template and add to /etc/supservisor/conf.d:

    https://github.com/okfn/ckan/blob/master/ckan/config/celery-supervisor.conf


Configuration
-------------

The only configuration that is is nee

    ckan.webstore_url=http://0.0.0.0:5555


Developers
----------

You can run the test suite from the ckanext-webstorer directory.
The tests require nose, so install it first if you have not already
done so:

::

   $ pip install nose

Then, run nosetests from the ckanext-archiver directory

::

   $ nosetests
