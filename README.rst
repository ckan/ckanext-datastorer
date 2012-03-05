CKAN Datastorer Extension
=======================

The CKAN Debstorer Extension provides a Celery task for automatically
 saving ckan resources that link to csv and excel files into the datastore.


Installation
------------

Install the plugin using pip. Download the source, then
from the ckanext-datastorer directory, run

::

    $ pip install -e ./
    $ pip install -r requires.txt

Start the celery deamon.  This can be done in development by::

    paster celeryd # this is assuming a development.ini file

In production the deamon should be run with a different ini file and be run as an init script.
The simplist way to do this is to install supervisor::

    apt-get insatll supervisor

Using this file as a template and add to /etc/supservisor/conf.d:

    https://github.com/okfn/ckan/blob/master/ckan/config/celery-supervisor.conf



Developers
----------

You can run the test suite from the ckanext-datastorer directory.
The tests require nose, so install it first if you have not already
done so:

::

   $ pip install nose

Then, run nosetests from the ckanext-archiver directory

::

   $ nosetests tests

