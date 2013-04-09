from setuptools import setup, find_packages

version = '0.1'

setup(
    name='ckanext-datastorer',
    version=version,
    description="Tasks that upload data to the DataStore.",
    long_description="""\
    """,
    classifiers=[],  # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
    keywords='',
    author='CKAN team.',
    author_email='ckan-dev@okfn.org',
    url='https://github.com/okfn/ckenext-datastorer',
    license='AGPL',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=['ckanext', 'ckanext.datastorer'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        # Requirements defined in pip-requirements.txt
    ],
    entry_points="""
    [paste.paster_command]
    datastorer = ckanext.datastorer.commands:Datastorer
    datastore_upload = ckanext.datastorer.commands:AddToDataStore

    [ckan.plugins]
    datastorer = ckanext.datastorer.plugin:DatastorerPlugin

    [ckan.celery_task]
    tasks = ckanext.datastorer.celery_import:task_imports
    """,
)
