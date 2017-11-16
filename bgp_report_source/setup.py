from setuptools import setup

setup(name='bgp_report_source',
      version='0.1',
      description='routeflow-bgp-analysis',
      url='https://github.com/GlobalNOC/routeflow-bgp-analysis/tree/master/bgpReport_source',
      author='Mihir Thatte',
      author_email='mthatte@indiana.edu',
      license='',
      packages=['bgp_report_source'],
      package_data={'bgp_report_source': ['config.json']},
      include_package_data=True,
      install_requires=[
          'simplejson','elasticsearch','mrtparse','wget',
      ],
      zip_safe=False)
