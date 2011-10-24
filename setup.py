from setuptools import setup, find_packages
import sys, os

version = '0.0'

setup(
	name='ckanext-webstorer',
	version=version,
	description="Tasks that upload data to the webstore.",
	long_description="""\
	""",
	classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
	keywords='',
	author='CKAN team.',
	author_email='ckan-dev@okfn.org',
	url='https://github.com/okfn/ckenext-webstorer',
	license='AGPL',
	packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
	namespace_packages=['ckanext', 'ckanext.webstorer'],
	include_package_data=True,
	zip_safe=False,
    install_requires = [
        'messytables>=0.1.1',
    ],
	entry_points=\
	"""
        [ckan.plugins]
	# Add plugins here, eg
	# myplugin=ckanext.webstorer:PluginClass
	""",
)
