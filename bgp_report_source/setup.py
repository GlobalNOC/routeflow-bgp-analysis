from setuptools import setup

def readme():
    with open('README.rst') as f:
        return f.read()

setup(name='bgp_report_source',
      version='0.1',
      description='routeflow-bgp-analysis',
      url='https://github.com/GlobalNOC/routeflow-bgp-analysis/',
      author='Mihir Thatte',
      author_email='mthatte@indiana.edu',
      license='',
      packages=['bgp_report_source'],
      package_data={'bgp_report_source': ['config.json']},
      include_package_data=True,
      install_requires=[
          'simplejson','elasticsearch','mrtparse','wget',
      ],
      entry_points = {
        'console_scripts': ['bgp-report-run=bgp_report_source.command_line:main'],
      },
      zip_safe=False)
