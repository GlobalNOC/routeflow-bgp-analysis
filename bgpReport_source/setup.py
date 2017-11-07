from setuptools import setup

setup(name='bgpReport_source',
      version='0.1',
      description='routeflow-bgp-analysis',
      url='https://github.com/GlobalNOC/routeflow-bgp-analysis/tree/master/bgpReport_source',
      author='Mihir Thatte',
      author_email='mthatte@indiana.edu',
      license='',
      packages=['bgpReport_source'],
      package_data={'bgpReport_source': ['config.json']},
      include_package_data=True,
      install_requires=[
          'simplejson','elasticsearch','mrtparse','wget',
      ],
      zip_safe=False)
