CKAN Datastorer Extension
=======================

The CKAN Debstorer Extension provides a Celery task for automatically
 saving ckan resources that link to csv and excel files into the datastore.


Installation
------------

After activating your pyenv, install the sources via pip::

    $ (pyenv) pip install -e git+git://github.com/okfn/ckanext-datastorer.git#egg=ckanext-datastorer

Install the requirements::

    $ (pyenv) pip install -r ckanext-datastorer/requires.txt

Add the datastorer plugin to your configuration ini file::

    ckan.plugins = datastorer <rest of plugins>...

Start the celery daemon.  This can be done in development by::

    paster celeryd # this is assuming a development.ini file

In production the daemon should be run with a different ini file and be run as an init script.
The simpliest way to do this is to install supervisor::

    apt-get install supervisor

You can use this file as a template and add it to /etc/supservisor/conf.d:

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

